"""
Scheduling strategy implementations for ScheduleMesh.
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import Any, Callable, Mapping, MutableMapping, Optional, Sequence, Type, Union

AgentRecord = MutableMapping[str, Any]
StrategyFactory = Callable[[], "SchedulingStrategy"]
StrategyResolver = Callable[[Optional[Mapping[str, str]]], "SchedulingStrategy"]
StrategySpec = Union[
    str,
    "SchedulingStrategy",
    Type["SchedulingStrategy"],
    StrategyFactory,
]


class SchedulingStrategy(ABC):
    """Base class for all scheduling strategies."""

    @abstractmethod
    def select(self, agents: Sequence[AgentRecord]) -> AgentRecord:
        """Select a single agent from the available list."""

    @staticmethod
    def _ensure_agents(agents: Sequence[AgentRecord]) -> Sequence[AgentRecord]:
        if not agents:
            raise ValueError("Cannot select from an empty list of agents.")
        return agents


class RoundRobinStrategy(SchedulingStrategy):
    """Cycle through agents in a round-robin manner."""

    def __init__(self) -> None:
        self._index = 0

    def select(self, agents: Sequence[AgentRecord]) -> AgentRecord:
        agents = self._ensure_agents(agents)
        agent = agents[self._index % len(agents)]
        self._index = (self._index + 1) % len(agents)
        return agent


class LabelRoundRobinStrategy(SchedulingStrategy):
    """
    根据给定标签挑选 Agent，并在匹配集合中依次轮询。

    如果未设置标签，则回退为普通的全量轮询策略。
    """

    def __init__(self, labels: Optional[Mapping[str, str]] = None) -> None:
        self._target_labels: dict[str, str] = dict(labels or {})
        self._label_index = 0
        self._global_index = 0

    def set_labels(self, labels: Optional[Mapping[str, str]]) -> None:
        """更新当前使用的标签过滤条件。"""
        self._target_labels = dict(labels or {})
        self._label_index = 0

    def _matching_agents(self, agents: Sequence[AgentRecord]) -> list[AgentRecord]:
        if not self._target_labels:
            return list(agents)

        matching: list[AgentRecord] = []
        for agent in agents:
            labels = agent.get("labels")
            if not isinstance(labels, Mapping):
                continue
            if all(labels.get(key) == value for key, value in self._target_labels.items()):
                matching.append(agent)
        return matching

    def select(self, agents: Sequence[AgentRecord]) -> AgentRecord:
        agents = self._ensure_agents(agents)
        filtered = self._matching_agents(agents)

        if not filtered:
            # 无可用 agent 与标签匹配时立即报错，提示上层调整筛选条件。
            description = ", ".join(f"{key}={value}" for key, value in sorted(self._target_labels.items()))
            if not description:
                description = "<none>"
            raise ValueError(f"No agents match label filters ({description}).")

        pool = filtered if self._target_labels else agents
        if self._target_labels:
            agent = pool[self._label_index % len(pool)]
            self._label_index = (self._label_index + 1) % len(pool)
            return agent

        agent = pool[self._global_index % len(pool)]
        self._global_index = (self._global_index + 1) % len(pool)
        return agent


def _queue_length(agent: Mapping[str, Any]) -> float:
    metrics = agent.get("metrics")
    if isinstance(metrics, Mapping):
        value = metrics.get("queue_length", 0)
        if isinstance(value, (int, float)):
            return float(value)
    return 0.0


class LeastBusyStrategy(SchedulingStrategy):
    """Pick the agent reporting the smallest queue length."""

    def select(self, agents: Sequence[AgentRecord]) -> AgentRecord:
        agents = self._ensure_agents(agents)
        return min(agents, key=_queue_length)


class RandomStrategy(SchedulingStrategy):
    """Select an agent uniformly at random."""

    def select(self, agents: Sequence[AgentRecord]) -> AgentRecord:
        agents = self._ensure_agents(agents)
        return random.choice(list(agents))


class PowerOfTwoStrategy(SchedulingStrategy):
    """Sample two agents at random and return the less busy one."""

    def select(self, agents: Sequence[AgentRecord]) -> AgentRecord:
        agents = self._ensure_agents(agents)
        if len(agents) == 1:
            return agents[0]

        candidates = random.sample(list(agents), k=min(2, len(agents)))
        return min(candidates, key=_queue_length)


def _coerce_strategy_instance(candidate: SchedulingStrategy | Any) -> SchedulingStrategy:
    if isinstance(candidate, SchedulingStrategy):
        return candidate
    raise TypeError("Factory did not return a SchedulingStrategy instance.")


_STRATEGY_REGISTRY: dict[str, StrategyResolver] = {}


def register_strategy(
    name: str,
    factory: StrategyResolver,
    *,
    replace: bool = False,
) -> None:
    """
    将调度策略注册到全局表中。

    Args:
        name: 策略名称，将被标准化为小写。
        factory: 根据 ``labels`` 上下文返回策略实例的工厂方法。
        replace: 当名称已存在时是否允许覆盖，默认不允许。
    """
    key = name.strip().lower()
    if not key:
        raise ValueError("Strategy name must be a non-empty string.")
    if key in _STRATEGY_REGISTRY and not replace:
        raise ValueError(f"Strategy '{key}' already registered.")
    _STRATEGY_REGISTRY[key] = factory


def unregister_strategy(name: str) -> None:
    """从全局表删除指定名称的策略，名称不存在时静默返回。"""
    key = name.strip().lower()
    _STRATEGY_REGISTRY.pop(key, None)


def available_strategies() -> tuple[str, ...]:
    """返回当前已注册的策略名称列表（按字母序）。"""
    return tuple(sorted(_STRATEGY_REGISTRY))


def _resolve_registered_strategy(name: str, labels: Optional[Mapping[str, str]]) -> SchedulingStrategy:
    key = name.strip().lower()
    try:
        factory = _STRATEGY_REGISTRY[key]
    except KeyError as exc:
        raise ValueError(
            f"Unknown scheduling strategy '{name}'. "
            f"Available strategies: {', '.join(sorted(_STRATEGY_REGISTRY)) or '<none>'}"
        ) from exc
    return factory(labels)


def create_strategy(strategy: StrategySpec, *, labels: Optional[Mapping[str, str]] = None) -> SchedulingStrategy:
    """
    根据输入生成或校验调度策略实例。

    Args:
        strategy: 策略标识。支持：
          * 字符串名称（"round_robin"、"least_busy"、"random"、"power_of_two"）
          * 支持标签过滤的轮询策略（"label_round_robin"，可结合 ``labels`` 上下文）
          * SchedulingStrategy 的子类
          * 返回 SchedulingStrategy 的工厂方法
          * 已存在的 SchedulingStrategy 实例（直接返回）
        labels: 可选标签过滤，主要用于标签轮询策略。
    """

    if isinstance(strategy, SchedulingStrategy):
        return strategy

    if isinstance(strategy, str):
        return _resolve_registered_strategy(strategy, labels)

    if isinstance(strategy, type) and issubclass(strategy, SchedulingStrategy):
        if issubclass(strategy, LabelRoundRobinStrategy):
            return strategy(labels=labels)
        return strategy()

    if callable(strategy):
        return _coerce_strategy_instance(strategy())

    raise TypeError(
        "Strategy must be provided as a name, SchedulingStrategy subclass, "
        "callable factory, or SchedulingStrategy instance."
    )


register_strategy("round_robin", lambda _: RoundRobinStrategy())
register_strategy("least_busy", lambda _: LeastBusyStrategy())
register_strategy("random", lambda _: RandomStrategy())
register_strategy("power_of_two", lambda _: PowerOfTwoStrategy())
register_strategy("label_round_robin", lambda labels: LabelRoundRobinStrategy(labels=labels))


__all__ = [
    "SchedulingStrategy",
    "RoundRobinStrategy",
    "LabelRoundRobinStrategy",
    "LeastBusyStrategy",
    "RandomStrategy",
    "PowerOfTwoStrategy",
    "available_strategies",
    "register_strategy",
    "unregister_strategy",
    "create_strategy",
]
