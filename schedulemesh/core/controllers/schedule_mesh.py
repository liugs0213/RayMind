"""
High-level ScheduleMesh façade that exposes mesh-oriented invocation helpers.

This module provides a synchronous API on top of the supervisor-backed
``RayScheduler`` for common execution patterns:

* ``all()``    – broadcast the same method call to every agent in the selection.
* ``choose()`` – pick a single agent (round-robin) for the call.
* ``shard()``  – fan out shards to agents with an optional custom dispatcher.

The implementation is intentionally lightweight: the heavy lifting remains in
the underlying Ray actors.  The façade focuses on ergonomics and protecting
callers against missing resources, while preserving room for future expansion
(metrics-based routing, richer dispatch policies, etc.).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import ray

from schedulemesh.core.config import get_scheduling_config
from schedulemesh.core.controllers.ray_scheduler import RayScheduler
from schedulemesh.core.scheduling.strategy import (
    LabelRoundRobinStrategy,
    SchedulingStrategy,
    create_strategy,
)

AgentRecord = Dict[str, Any]
DispatchSpec = Iterable[Tuple[Union[int, AgentRecord], Sequence[Any], MutableMapping[str, Any]]]
DispatchFn = Callable[[Sequence[AgentRecord], Tuple[Any, ...], Dict[str, Any]], DispatchSpec]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Selection:
    """Internal description of a mesh selection."""

    pool: Optional[str]
    labels: Tuple[Tuple[str, str], ...]

    @property
    def key(self) -> Tuple[Optional[str], Tuple[Tuple[str, str], ...]]:
        return self.pool, self.labels


class _BaseProxy:
    """Shared helper that proxies method invocations to Ray agent handles."""

    def __init__(self, agents: Sequence[AgentRecord]):
        if not agents:
            raise RuntimeError("No agents available for the requested selection")
        self._agents = list(agents)

    def _remote_invoke(self, agent: AgentRecord, method: str, args: Sequence[Any], kwargs: Dict[str, Any]):
        handle = agent.get("handle")
        if handle is None:
            raise RuntimeError(f"Agent '{agent.get('name')}' is missing a Ray handle")
        return handle.invoke.remote(method, *args, **kwargs)

    def _wrap_method(self, method: str, executor: Callable[..., Any]):
        def _call(*args: Any, **kwargs: Any) -> Any:
            return executor(method, *args, **dict(kwargs))

        return _call

    def invoke(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """Fallback entry-point mirroring ``handle.invoke``."""
        raise NotImplementedError


class _AllProxy(_BaseProxy):
    """Broadcast the same call to every agent and aggregate the results."""

    def __getattr__(self, item: str):
        return self._wrap_method(item, self.invoke)

    def invoke(self, method: str, *args: Any, **kwargs: Any) -> List[Any]:
        logger.debug(
            "ScheduleMesh.all invoking method=%s agents=%s",
            method,
            [agent.get("name") for agent in self._agents],
        )
        refs = [self._remote_invoke(agent, method, args, kwargs) for agent in self._agents]
        return list(ray.get(refs))


class _ChooseProxy(_BaseProxy):
    """Select a single agent for each invocation using a scheduling strategy."""

    def __init__(self, agents: Sequence[AgentRecord], strategy: SchedulingStrategy):
        super().__init__(agents)
        self._strategy = strategy

    def __getattr__(self, item: str):
        return self._wrap_method(item, self.invoke)

    def _pick_agent(self) -> AgentRecord:
        """Pick an agent using the configured scheduling strategy."""
        return self._strategy.select(self._agents)

    def invoke(self, method: str, *args: Any, **kwargs: Any) -> Any:
        agent = self._pick_agent()
        logger.debug("ScheduleMesh.choose selected agent=%s method=%s", agent.get("name"), method)
        ref = self._remote_invoke(agent, method, args, kwargs)
        return ray.get(ref)


class _ShardedProxy(_BaseProxy):
    """Fan out shards to agents either via a custom dispatcher or a default rule."""

    def __init__(
        self,
        agents: Sequence[AgentRecord],
        dispatch_fn: Optional[DispatchFn] = None,
    ):
        super().__init__(agents)
        self._dispatch_fn = dispatch_fn

    def __getattr__(self, item: str):
        return self._wrap_method(item, self.invoke)

    def _default_dispatch(
        self,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
    ) -> List[Tuple[AgentRecord, Tuple[Any, ...], Dict[str, Any]]]:
        shards = kwargs.pop("shards", None)
        remaining_args = list(args)
        if shards is None and remaining_args:
            candidate = remaining_args[0]
            if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
                shards = candidate
                remaining_args = remaining_args[1:]

        if shards is None:
            raise ValueError("Sharded call requires either 'shards' kwarg or leading positional shards sequence")

        shards_list = list(shards)
        if len(shards_list) != len(self._agents):
            raise ValueError("Number of shards must match number of agents in the selection")

        plan: List[Tuple[AgentRecord, Tuple[Any, ...], Dict[str, Any]]] = []
        for agent, shard in zip(self._agents, shards_list):
            agent_args = (shard, *remaining_args)
            agent_kwargs = dict(kwargs)
            plan.append((agent, agent_args, agent_kwargs))
        return plan

    def _build_dispatch_plan(
        self,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
    ) -> List[Tuple[AgentRecord, Tuple[Any, ...], Dict[str, Any]]]:
        kwargs_copy = dict(kwargs)
        if self._dispatch_fn is None:
            return self._default_dispatch(args, kwargs_copy)

        plan: List[Tuple[AgentRecord, Tuple[Any, ...], Dict[str, Any]]] = []
        for target, call_args, call_kwargs in self._dispatch_fn(self._agents, args, kwargs_copy):
            if isinstance(target, int):
                try:
                    agent = self._agents[target]
                except IndexError as exc:
                    raise ValueError(f"Dispatch target index {target} out of range") from exc
            else:
                agent = target
            plan.append((agent, tuple(call_args), dict(call_kwargs)))
        if not plan:
            raise ValueError("Dispatch plan must target at least one agent")
        return plan

    def invoke(self, method: str, *args: Any, **kwargs: Any) -> List[Any]:
        plan = self._build_dispatch_plan(tuple(args), dict(kwargs))
        logger.debug(
            "ScheduleMesh.shard dispatching method=%s plan=%s",
            method,
            [
                {"agent": agent.get("name"), "args": agent_args, "kwargs": agent_kwargs}
                for agent, agent_args, agent_kwargs in plan
            ],
        )
        refs = [self._remote_invoke(agent, method, agent_args, agent_kwargs) for agent, agent_args, agent_kwargs in plan]
        return list(ray.get(refs))


class ScheduleMesh:
    """Client-facing mesh façade built on top of :class:`RayScheduler`."""

    def __init__(
        self,
        scheduler: RayScheduler,
        *,
        default_pool: Optional[str] = None,
        default_labels: Optional[Dict[str, str]] = None,
    ):
        self._scheduler = scheduler
        self._default_selection = _Selection(
            pool=default_pool,
            labels=tuple(sorted((default_labels or {}).items())),
        )
        self._default_strategy = get_scheduling_config().default_strategy

    @property
    def scheduler(self) -> RayScheduler:
        return self._scheduler

    def _selection(self, pool: Optional[str], labels: Optional[Dict[str, str]]) -> _Selection:
        effective_pool = pool if pool is not None else self._default_selection.pool
        effective_labels = tuple(sorted((labels or dict(self._default_selection.labels)).items()))
        return _Selection(pool=effective_pool, labels=effective_labels)

    def _resolve_agents(self, selection: _Selection) -> List[AgentRecord]:
        # 请求包含handle的agent信息，因为需要调用远程方法
        if selection.pool is not None:
            response = self._scheduler.list_agents(selection.pool, include_handle=True)
        else:
            response = self._scheduler.list_agents(include_handle=True)

        if not response.get("success"):
            raise RuntimeError(response.get("error") or "Failed to list agents from scheduler")

        raw_agents: List[AgentRecord] = list(response.get("agents", []))
        filter_labels = dict(selection.labels)
        pool_label_cache: Dict[str, Dict[str, str]] = {}

        def _merged_labels(agent: AgentRecord) -> Dict[str, str]:
            labels = dict(agent.get("labels") or {})
            pool_name = labels.get("pool")
            if pool_name and pool_name not in pool_label_cache:
                pool_response = self._scheduler.get_pool(pool_name)
                if pool_response.get("success"):
                    pool_data = pool_response.get("pool") or {}
                    pool_label_cache[pool_name] = dict(pool_data.get("labels") or {})
                else:  # cache empty dict to avoid repeat calls on error
                    pool_label_cache[pool_name] = {}
            merged = dict(pool_label_cache.get(pool_name, {}))
            merged.update(labels)
            return merged

        agents: List[AgentRecord] = []
        for agent in raw_agents:
            effective_labels = _merged_labels(agent)
            if filter_labels and not filter_labels.items() <= effective_labels.items():
                continue
            enriched = dict(agent)
            # 保留合并后的标签视图，方便下游策略直接复用。
            enriched["labels"] = effective_labels
            agents.append(enriched)
        logger.debug(
            "ScheduleMesh selection resolved agents=%s filters=%s pool=%s",
            [agent.get("name") for agent in agents],
            dict(selection.labels),
            selection.pool,
        )
        return agents

    def all(
        self,
        *,
        pool: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> _AllProxy:
        """Broadcast identical calls to every agent in the selection."""
        selection = self._selection(pool, labels)
        agents = self._resolve_agents(selection)
        return _AllProxy(agents)

    def choose(
        self,
        *,
        pool: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        strategy: Optional[str] = None,
    ) -> _ChooseProxy:
        """
        Select a single agent from the selection using the specified strategy.
        
        Args:
            pool: Optional pool name to filter agents
            labels: Optional labels to filter agents
            strategy: Scheduling strategy to use. If omitted, use configured default.
                Built-ins include:
                ``least_busy``, ``round_robin``, ``random``, ``power_of_two``, ``label_round_robin``.
        """
        selection = self._selection(pool, labels)
        agents = self._resolve_agents(selection)
        selection_labels = dict(selection.labels)
        target_labels = selection_labels or None
        strategy_name = strategy if strategy is not None else self._default_strategy
        strategy_instance = create_strategy(strategy_name, labels=target_labels)
        if isinstance(strategy_instance, LabelRoundRobinStrategy) and target_labels is not None:
            # 策略实例需要同步当前筛选条件，保证轮询窗口一致。
            strategy_instance.set_labels(target_labels)
        return _ChooseProxy(agents, strategy_instance)

    def shard(
        self,
        *,
        pool: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        dispatch_fn: Optional[DispatchFn] = None,
    ) -> _ShardedProxy:
        """
        Fan out shards across agents.

        Without a ``dispatch_fn`` the call expects either:

        * A ``shards`` keyword argument containing an iterable matching the number of agents.
        * Or a leading positional argument that is such an iterable.
        """
        selection = self._selection(pool, labels)
        agents = self._resolve_agents(selection)
        return _ShardedProxy(agents, dispatch_fn=dispatch_fn)
