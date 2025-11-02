"""
Client-facing RayScheduler façade.

The façade proxies all operations to the underlying supervisor actor while
exposing a synchronous API to library consumers.
"""

from __future__ import annotations

from typing import Any, Optional

import ray

from schedulemesh.core.actors.control.supervisor import RaySchedulerSupervisorActor
from schedulemesh.core.actors.management.config import ActorConfig


class RayScheduler:
    """Thin wrapper around the RaySchedulerSupervisorActor."""

    def __init__(
        self,
        name: str = "schedulemesh-supervisor",
        *,
        namespace: str | None = None,
        detached: bool = False,
        max_restarts: int = -1,
        max_task_retries: int = 0,
        state_path: str | None = None,
    ):
        """
        Create (and bootstrap) a new supervisor actor.

        Args:
            name: Logical name for the supervisor actor.
            namespace: Ray namespace to place the actor in. ``None`` uses
                the caller's current namespace.
            detached: Whether to create the supervisor as a detached actor.
                Detached actors survive driver exits and can be attached to
                from new processes.
            max_restarts: Passed to Ray to automatically restart the actor
                on failure (``-1`` means infinite restarts).
            max_task_retries: Maximum automatic retries for actor tasks.
        """
        self.name = name
        self._namespace = namespace
        self._state_path = state_path
        actor_config = ActorConfig(name=name, state_path=state_path)
        actor_options: dict[str, Any] = {}
        if namespace is not None:
            actor_options["namespace"] = namespace
        if detached:
            actor_options.update(
                {
                    "name": name,
                    "lifetime": "detached",
                    "max_restarts": max_restarts,
                    "max_task_retries": max_task_retries,
                }
            )
        self._supervisor = RaySchedulerSupervisorActor.options(**actor_options).remote(
            actor_config
        )
        self._owns_supervisor = True
        ray.get(self._supervisor.bootstrap.remote())

    @classmethod
    def attach(
        cls,
        name: str = "schedulemesh-supervisor",
        *,
        namespace: str | None = None,
        state_path: str | None = None,
    ) -> "RayScheduler":
        """
        Attach to an existing supervisor actor (typically detached).
        """
        handle = ray.get_actor(name, namespace=namespace)
        instance = cls.__new__(cls)
        instance.name = name
        instance._namespace = namespace
        instance._state_path = state_path
        instance._supervisor = handle
        instance._owns_supervisor = False
        return instance

    def _ensure_supervisor(self) -> ray.actor.ActorHandle:
        if self._supervisor is None:
            raise RuntimeError("RayScheduler has been shut down")
        return self._supervisor

    def supervisor_handle(self) -> ray.actor.ActorHandle:
        """
        返回底层的 supervisor actor 句柄，常用于 Agent 侧的指标上报等高级用法。
        """
        return self._ensure_supervisor()

    def create_pool(
        self,
        name: str,
        labels: dict[str, str],
        resources: dict[str, float],
        *,
        target_agents: int = 0,
        placement_strategy: str = "STRICT_PACK",
        pg_pool_config: Optional[dict] = None,
    ) -> Any:
        if pg_pool_config and pg_pool_config.get("enable") and pg_pool_config.get("high_priority_pg_specs"):
            def _to_float(value: Any) -> float:
                return float(value) if value is not None else 0.0

            def spec_total(spec: dict) -> tuple[float, float, float]:
                return (
                    _to_float(spec.get("cpu")),
                    _to_float(spec.get("memory")),
                    _to_float(spec.get("gpu")),
                )

            pool_cpu = _to_float(resources.get("cpu"))
            pool_mem = _to_float(resources.get("memory"))
            pool_gpu = _to_float(resources.get("gpu"))

            for idx, spec in enumerate(pg_pool_config["high_priority_pg_specs"]):
                spec_cpu, spec_mem, spec_gpu = spec_total(spec)
                if spec_cpu > pool_cpu or spec_mem > pool_mem or spec_gpu > pool_gpu:
                    raise ValueError(
                        f"High-priority PG spec[{idx}] exceeds pool quota: "
                        f"spec(cpu={spec_cpu}, memory={spec_mem}, gpu={spec_gpu}) "
                        f"> pool(cpu={pool_cpu}, memory={pool_mem}, gpu={pool_gpu})"
                    )

        supervisor = self._ensure_supervisor()
        return ray.get(
            supervisor.create_resource_pool.remote(name, labels, resources, target_agents, placement_strategy, pg_pool_config)
        )

    def list_pools(self) -> Any:
        """列出当前注册的资源池。"""
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.list_pools.remote())

    def create_agent(
        self,
        name: str,
        pool: str,
        actor_class: type,
        *,
        resources: dict[str, float] | None = None,
        ray_options: dict[str, Any] | None = None,
        actor_args: list[Any] | None = None,
        actor_kwargs: dict[str, Any] | None = None,
    ) -> Any:
        supervisor = self._ensure_supervisor()
        extra_kwargs: dict[str, Any] = {}
        if resources is not None:
            extra_kwargs["resources_override"] = resources
        if ray_options is not None:
            extra_kwargs["ray_options"] = ray_options
        if actor_args is not None:
            extra_kwargs["actor_args"] = actor_args
        if actor_kwargs is not None:
            extra_kwargs["actor_kwargs"] = actor_kwargs

        return ray.get(
            supervisor.create_agent.remote(
                name,
                {"pool": pool},
                actor_class,
                **extra_kwargs,
            )
        )

    def list_agents(self, pool: str | None = None, *, include_handle: bool = False) -> Any:
        """列出 Agent 列表，可选限制到指定 pool。"""
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.list_agents.remote(pool, include_handle=include_handle))

    def delete_agent(self, name: str, *, force: bool = False, destroy_pg: bool = True) -> Any:
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.delete_agent.remote(name, force=force, destroy_pg=destroy_pg))

    def list_agents(self, pool: str | None = None, *, include_handle: bool = False) -> Any:
        """
        获取Agent列表
        
        Args:
            pool: 可选的资源池名称过滤
            include_handle: 是否包含Ray actor句柄（默认False）
        
        Returns:
            Agent信息字典
        """
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.list_agents.remote(pool, include_handle=include_handle))

    def get_pool(self, name: str) -> Any:
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.get_pool.remote(name))

    def submit_task(
        self,
        label: str,
        payload: Any,
        *,
        labels: dict[str, str] | None = None,
        strategy: str | None = None,
        priority: float = 0.0,
        task_id: str | None = None,
    ) -> Any:
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.submit_task.remote(label, payload, labels, strategy, priority, task_id))

    def choose_task(self, label: str) -> Any:
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.choose_task.remote(label))

    def update_agent_metrics(self, name: str, metrics: dict[str, Any]) -> Any:
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.update_agent_metrics.remote(name, metrics))

    # Preemption operations --------------------------------------------------

    def evaluate_preemption(
        self,
        incoming_task_priority: float,
        incoming_task_pool: str,
        incoming_task_labels: dict[str, str] | None = None,
        incoming_task_resources: dict[str, float] | None = None,
    ) -> Any:
        """评估抢占候选"""
        supervisor = self._ensure_supervisor()
        return ray.get(
            supervisor.evaluate_preemption.remote(
                incoming_task_priority,
                incoming_task_pool,
                incoming_task_labels,
                incoming_task_resources,
            )
        )

    def execute_preemption(self, task_id: str, agent_name: str | None = None) -> Any:
        """执行抢占操作"""
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.execute_preemption.remote(task_id, agent_name))

    def register_running_task(
        self,
        task_id: str,
        agent_name: str,
        pool_name: str,
        priority: float,
        labels: dict[str, str] | None = None,
        estimated_duration: float = 0.0,
        payload: Any = None,
        resources: dict[str, float] | None = None,
    ) -> Any:
        """注册运行中的任务"""
        supervisor = self._ensure_supervisor()
        return ray.get(
            supervisor.register_running_task.remote(
                task_id,
                agent_name,
                pool_name,
                priority,
                labels,
                estimated_duration,
                payload,
                resources,
            )
        )

    def unregister_task(self, task_id: str) -> Any:
        """任务完成，取消注册"""
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.unregister_task.remote(task_id))

    def get_preemption_stats(self) -> Any:
        """获取抢占统计"""
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.get_preemption_stats.remote())
    
    def submit_task_with_pg_preemption(self, **kwargs: Any) -> Any:
        """提交任务，使用 PG 池支持快速抢占（推荐方式）"""
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.submit_task_with_pg_preemption.remote(**kwargs))
    
    def configure_pg_pool(self, pool_name: str, **kwargs: Any) -> Any:
        """配置资源池的 PlacementGroup 池"""
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.configure_pg_pool.remote(pool_name, **kwargs))
    
    def get_pg_pool_stats(self, pool_name: Optional[str] = None) -> Any:
        """获取 PG 池统计信息"""
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.get_pg_pool_stats.remote(pool_name))

    def register_state_handlers(
        self,
        pool_name: str,
        preserver: Any,
        restorer: Any,
    ) -> Any:
        """注册自定义状态保存/恢复处理器"""
        supervisor = self._ensure_supervisor()
        return ray.get(
            supervisor.register_state_handlers.remote(pool_name, preserver, restorer)
        )

    def restore_task(self, saved_state: Any) -> Any:
        """通过自定义处理器恢复任务状态"""
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.restore_task.remote(saved_state))

    def ensure_agent_health(self, timeout: float = 120.0, recreate: bool = True) -> Any:
        """检测 Agent 心跳并根据需要执行清理/重建"""
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.reconcile_agents.remote(timeout, recreate))

    def update_preemption_policy(self, **kwargs: Any) -> Any:
        """
        动态更新抢占策略。

        此方法允许在运行时调整抢占控制器的行为，支持细粒度的数值参数
        以及语义化的抢占级别配置。

        Args:
            preemption_aggressiveness (PreemptionAggressiveness | str, optional):
                设置抢占的积极性级别，可选值为 ``high``/``medium``/``low``，
                或直接使用 :class:`~schedulemesh.config.policy.PreemptionAggressiveness` 枚举。
                此参数会自动配置底层的数值阈值，简化配置。
                - "high": 更容易抢占，适用于需要快速响应的场景。
                - "medium": 默认级别，平衡抢占和稳定性。
                - "low": 更不容易抢占，适用于需要任务稳定运行的场景。
            
            enable_label_preemption (bool, optional): 是否启用基于标签的抢占。
            label_preemption_rules (dict, optional): 定义标签抢占的规则。
            
            其他 PreemptionPolicy 中定义的参数亦可在此处直接设置。

        Returns:
            更新后的策略配置。
        """
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.update_preemption_policy.remote(**kwargs))

    def preempt_task(
        self,
        *,
        incoming_task_priority: float,
        incoming_task_pool: str,
        incoming_task_labels: dict[str, str] | None = None,
        incoming_task_resources: dict[str, float] | None = None,
        target_task_id: str | None = None,
        target_agent_name: str | None = None,
    ) -> Any:
        """简化的抢占入口：评估并执行"""
        supervisor = self._ensure_supervisor()
        return ray.get(
            supervisor.preempt_task.remote(
                incoming_task_priority,
                incoming_task_pool,
                incoming_task_labels,
                incoming_task_resources,
                target_task_id,
                target_agent_name,
            )
        )

    def submit_task_with_preemption(self, **kwargs: Any) -> Any:
        """
        提交一个任务，并在资源不足时自动触发抢占。

        这是推荐的任务提交方式，它封装了资源检查、抢占和Agent创建的
        完整逻辑，简化了客户端的操作。

        Args:
            task_id (str): 任务的唯一标识。
            pool_name (str): 任务目标资源池。
            resources (dict): 任务所需的资源 (e.g., {"cpu": 1, "gpu": 0.5})。
            priority (float): 任务的数值优先级。
            labels (dict): 任务的标签，用于基于Label的抢占。
            actor_class (type): 要为任务实例化的Ray Actor类。
            actor_args (list, optional): 传递给Actor构造函数的位置参数。
            actor_kwargs (dict, optional): 传递给Actor构造函数的关键字参数。
            agent_name (str, optional): Agent的名称，如果未提供则自动生成。
                注意：调度器会始终将 `name` 和 `labels` 作为前两个参数传递给
                `actor_class`，`actor_args`/`actor_kwargs` 会追加在其后。

        Returns:
            一个字典，包含任务提交结果:
            {
                "success": bool,
                "agent": dict,  // 成功时返回创建的Agent信息
                "error": str,   // 失败时的错误信息
                "reason": str   // 失败原因 (e.g., "insufficient_resources", "preemption_failed")
            }
        """
        supervisor = self._ensure_supervisor()
        return ray.get(supervisor.submit_task_with_preemption.remote(**kwargs))

    def shutdown(self) -> None:
        if self._supervisor is None:
            return
        if self._owns_supervisor:
            ray.kill(self._supervisor, no_restart=True)
        self._supervisor = None  # type: ignore[assignment]
