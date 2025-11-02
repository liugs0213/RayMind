"""
SimpleScheduler
---------------

Provides a thin, user-friendly wrapper around :class:`schedulemesh.core.controllers.RayScheduler`.

The goal is to reduce the amount of boilerplate required to submit tasks with
preemption while keeping advanced capabilities (custom agents, label rules,
etc.) accessible through the underlying scheduler when needed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence, Type

import ray

from schedulemesh.core.controllers.ray_scheduler import RayScheduler


@dataclass
class SimpleTaskSpec:
    """
    Declarative description of a task submitted via :class:`SimpleScheduler`.

    Attributes:
        task_id: Unique identifier for the task.
        pool: Target resource pool name.
        actor_class: Ray actor class used to execute the task.
        resources: Resource requirements (cpu, gpu, memory, etc.).
        priority: Numerical priority used for scheduling and preemption.
        labels: Optional semantic labels (e.g., {"tier": "premium"}).
        actor_args: Optional positional arguments passed to the actor constructor
        actor_kwargs: Optional keyword arguments passed to the actor constructor.
        agent_name: Optional explicit agent name; auto-generated if omitted.
        ray_options: Optional dict forwarded to `ray.remote(...).options(**ray_options)`.
        estimated_duration: Expected runtime in seconds, used to improve preemption
            scoring when auto-registration is enabled.
        auto_register: When True (default), the running task is automatically
            registered with the preemption controller after successful submission.
    """

    task_id: str
    pool: str
    actor_class: Type[Any]
    resources: Mapping[str, float]
    priority: float = 5.0
    labels: Optional[Mapping[str, str]] = None
    actor_args: Optional[Sequence[Any]] = None
    actor_kwargs: Optional[Mapping[str, Any]] = None
    agent_name: Optional[str] = None
    ray_options: Optional[Mapping[str, Any]] = None
    estimated_duration: float = 0.0
    auto_register: bool = True

    def to_submission_kwargs(self) -> Dict[str, Any]:
        """Translate the spec into keyword arguments for RayScheduler."""
        submission_labels = dict(self.labels or {})
        kwargs: Dict[str, Any] = {
            "task_id": self.task_id,
            "pool_name": self.pool,
            "resources": dict(self.resources),
            "priority": self.priority,
            "labels": submission_labels,
            "actor_class": self.actor_class,
        }
        if self.actor_args:
            kwargs["actor_args"] = list(self.actor_args)
        if self.actor_kwargs:
            kwargs["actor_kwargs"] = dict(self.actor_kwargs)
        if self.agent_name:
            kwargs["agent_name"] = self.agent_name
        if self.ray_options:
            kwargs["ray_options"] = dict(self.ray_options)
        return kwargs


class SimpleScheduler:
    """
    High-level façade focused on the most common workstation / cluster flows.

    Typical usage::

        simple = SimpleScheduler()
        simple.ensure_pool("demo", resources={"cpu": 2, "memory": 4})
        result = simple.submit(
            task_id="job-1",
            pool="demo",
            actor_class=MyWorker,
            resources={"cpu": 1},
            labels={"tier": "standard"},
            estimated_duration=120,
        )
    """

    def __init__(self, name: str = "schedulemesh-simple", *, scheduler: Optional[RayScheduler] = None) -> None:
        self._scheduler = scheduler or RayScheduler(name)

    # ---------------------------------------------------------------------
    # Resource pool helpers
    # ---------------------------------------------------------------------

    def ensure_pool(
        self,
        name: str,
        *,
        labels: Optional[Mapping[str, str]] = None,
        resources: Optional[Mapping[str, float]] = None,
        target_agents: int = 0,
        placement_strategy: str = "STRICT_PACK",
        pg_pool_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create the pool if it does not exist, otherwise return the existing snapshot.

        Args:
            name: Resource pool name.
            labels: Pool labels (optional).
            resources: Default agent resources for the pool.
            target_agents: Desired number of pre-warmed agents.
            placement_strategy: Ray placement strategy.
            pg_pool_config: PlacementGroup pool configuration (optional).
        """
        labels_dict = dict(labels or {})
        resources_dict = dict(resources or {})

        snapshot = self._scheduler.get_pool(name)
        if snapshot.get("success"):
            return snapshot

        return self._scheduler.create_pool(
            name=name,
            labels=labels_dict,
            resources=resources_dict,
            target_agents=target_agents,
            placement_strategy=placement_strategy,
            pg_pool_config=pg_pool_config,
        )

    def list_pools(self) -> Dict[str, Any]:
        """列出当前所有注册的资源池。"""
        return self._scheduler.list_pools()

    # ---------------------------------------------------------------------
    # Task submission helpers
    # ---------------------------------------------------------------------

    def submit(
        self,
        *,
        task_id: str,
        pool: str,
        actor_class: Type[Any],
        resources: Mapping[str, float],
        priority: float = 5.0,
        labels: Optional[Mapping[str, str]] = None,
        actor_args: Optional[Sequence[Any]] = None,
        actor_kwargs: Optional[Mapping[str, Any]] = None,
        agent_name: Optional[str] = None,
        estimated_duration: float = 0.0,
        auto_register: bool = True,
        ray_options: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Convenience wrapper that mirrors :class:`SimpleTaskSpec` fields.
        """
        spec = SimpleTaskSpec(
            task_id=task_id,
            pool=pool,
            actor_class=actor_class,
            resources=resources,
            priority=priority,
            labels=labels,
            actor_args=actor_args,
            actor_kwargs=actor_kwargs,
            agent_name=agent_name,
            estimated_duration=estimated_duration,
            auto_register=auto_register,
            ray_options=ray_options,
        )
        return self.submit_spec(spec)
    
    def submit_with_pg_preemption(
        self,
        *,
        task_id: str,
        pool: str,
        actor_class: Type[Any],
        resources: Mapping[str, float],
        priority: float = 5.0,
        labels: Optional[Mapping[str, str]] = None,
        actor_args: Optional[Sequence[Any]] = None,
        actor_kwargs: Optional[Mapping[str, Any]] = None,
        estimated_duration: float = 0.0,
        auto_register: bool = True,
        ray_options: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Submit a task using PlacementGroup-based fast preemption (recommended).
        
        This method provides faster preemption by reusing PlacementGroups from the pool.
        """
        # Supervisor 内部已经根据 auto_register 参数自动注册到 PreemptionController
        # 这里不需要再次调用 register_running_task，否则会覆盖正确的资源信息
        submission = self._scheduler.submit_task_with_pg_preemption(
            task_id=task_id,
            pool_name=pool,
            actor_class=actor_class,
            resources=dict(resources),
            priority=priority,
            labels=dict(labels or {}),
            actor_args=list(actor_args) if actor_args else None,
            actor_kwargs=dict(actor_kwargs) if actor_kwargs else None,
            auto_register=auto_register,
            estimated_duration=estimated_duration,
            ray_options=dict(ray_options) if ray_options else None,
        )
        
        return submission

    def submit_spec(self, spec: SimpleTaskSpec) -> Dict[str, Any]:
        """
        Submit a task using a pre-built specification object.
        """
        submission = self._scheduler.submit_task_with_preemption(**spec.to_submission_kwargs())
        if submission.get("success") and spec.auto_register:
            agent_info = submission.get("agent", {})
            agent_name = agent_info.get("name")
            if agent_name:
                self._scheduler.register_running_task(
                    task_id=spec.task_id,
                    agent_name=agent_name,
                    pool_name=spec.pool,
                    priority=spec.priority,
                    labels=dict(spec.labels or {}),
                    estimated_duration=spec.estimated_duration,
                )
        return submission

    def complete(self, task_id: str) -> Dict[str, Any]:
        """
        Mark a running task as finished (unregisters from preemption tracking).
        """
        return self._scheduler.unregister_task(task_id)

    # ---------------------------------------------------------------------
    # Policy surface (delegated)
    # ---------------------------------------------------------------------

    def configure_preemption(self, **policy_kwargs: Any) -> Dict[str, Any]:
        """
        Update preemption policy using the underlying scheduler.

        Example::

            simple.configure_preemption(
                enable_label_preemption=True,
                label_preemption_rules={"tier": {"premium": ["standard"]}},
            )
        """
        return self._scheduler.update_preemption_policy(**policy_kwargs)

    def register_state_handlers(self, pool_name: str, preserver: Any, restorer: Any) -> Dict[str, Any]:
        """
        Convenience pass-through for advanced users who need custom state handling.
        """
        supervisor = self._scheduler._ensure_supervisor()  # pylint: disable=protected-access
        return ray.get(supervisor.register_state_handlers.remote(pool_name, preserver, restorer))

    # ---------------------------------------------------------------------
    # Introspection / lifecycle
    # ---------------------------------------------------------------------

    def stats(self) -> Dict[str, Any]:
        """Return preemption statistics."""
        return self._scheduler.get_preemption_stats()

    def list_agents(self, pool: Optional[str] = None, *, include_handle: bool = False) -> Dict[str, Any]:
        """列出 Agent 信息，可按 pool 过滤。"""
        return self._scheduler.list_agents(pool=pool, include_handle=include_handle)

    def pg_pool_stats(self, pool_name: Optional[str] = None) -> Dict[str, Any]:
        """Return PlacementGroup pool statistics."""
        return self._scheduler.get_pg_pool_stats(pool_name)

    def shutdown(self) -> None:
        """Shutdown underlying actors."""
        self._scheduler.shutdown()

    @property
    def scheduler(self) -> RayScheduler:
        """Expose the underlying scheduler for advanced scenarios."""
        return self._scheduler
