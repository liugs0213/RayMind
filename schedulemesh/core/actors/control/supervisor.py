"""
Supervisor actor skeleton.

This actor coordinates management/control actors, providing a single entry
point for the client-facing controller.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Optional, Dict

import ray

from schedulemesh.core.actors.management import ActorConfig, ResourcePoolManagerActor, AgentManagerActor
from schedulemesh.core.actors.control.placement_controller import PlacementControllerActor
from schedulemesh.core.actors.control.preemption_controller import PreemptionControllerActor
from schedulemesh.core.actors.control.scheduler_actor import SchedulerActor
from schedulemesh.core.actors.control.pg_pool_manager import PlacementGroupPoolManager
from schedulemesh.core.utils import configure_runtime_logging

logger = logging.getLogger(__name__)


@ray.remote
class RaySchedulerSupervisorActor:
    """Placeholder supervisor actor coordinating child actors."""

    def __init__(self, config: ActorConfig):
        configure_runtime_logging()
        self.config = config
        self.resource_pools: Optional[ray.actor.ActorHandle] = None
        self.agent_manager: Optional[ray.actor.ActorHandle] = None
        self.scheduler: Optional[ray.actor.ActorHandle] = None
        self.preemption: Optional[ray.actor.ActorHandle] = None
        self.placement: Optional[ray.actor.ActorHandle] = None
        self.pg_pool_manager: Optional[ray.actor.ActorHandle] = None  # PG Pool Manager
        logger.info("RaySchedulerSupervisorActor[%s] initialised", config.name)

    def bootstrap(self) -> dict:
        """Launch child actors with default configs."""
        # PG Pool Manager needs to be created before AgentManager
        self.pg_pool_manager = PlacementGroupPoolManager.remote()
        
        self.resource_pools = ResourcePoolManagerActor.remote(self.config)
        self.agent_manager = AgentManagerActor.remote(self.config, self.pg_pool_manager)
        scheduler_age = float(self.config.metadata.get("scheduler_aging_factor", 0.0001))
        self.scheduler = SchedulerActor.remote(scheduler_age)
        if self.config.state_path:
            ray.get(self.scheduler.configure.remote(self.config.state_path))
        self.preemption = PreemptionControllerActor.remote()
        # self.placement = PlacementControllerActor.remote()
        
        # 如果有状态路径，在恢复后清理无效的资源占用
        if self.config.state_path:
            self._cleanup_invalid_resource_usage()
        
        return {"success": True}

    def _cleanup_invalid_resource_usage(self) -> None:
        """清理无效的资源占用，确保恢复后的状态一致性"""
        if self.scheduler is None or self.resource_pools is None:
            return
            
        # 获取调度器中记录的 Agent 信息
        scheduler_agents = ray.get(self.scheduler.snapshot_state.remote()).get("agents", {})
        
        # 获取资源池中记录的已使用资源
        pools_snapshot = ray.get(self.resource_pools.snapshot_state.remote())
        
        for pool_data in pools_snapshot.get("pools", []):
            pool_name = pool_data.get("name")
            used_resources = pool_data.get("used_resources", {})
            if not pool_name or not used_resources:
                continue
                
            # 检查这个资源池中是否有对应的 Agent
            pool_agents = [agent for agent in scheduler_agents.values() 
                          if (agent.get("labels") or {}).get("pool") == pool_name]
            
            # 验证这些Agent是否真的存在（通过AgentManager检查）
            valid_agents = []
            for agent in pool_agents:
                agent_name = agent.get("name")
                if agent_name:
                    # 检查Agent是否真的存在
                    agent_exists = ray.get(self.agent_manager.get_agent.remote(agent_name)) is not None
                    if agent_exists:
                        valid_agents.append(agent)
                    else:
                        logger.debug("Agent %s 不存在，将被清理", agent_name)
            
            if not valid_agents:
                # 如果没有有效的 Agent，清理所有已使用的资源
                logger.info("清理资源池 %s 的无效资源占用", pool_name)
                ray.get(self.resource_pools.release_agent_slot.remote(pool_name, used_resources))
            else:
                # 计算实际需要的资源
                actual_used = {}
                for agent in valid_agents:
                    agent_resources = agent.get("resources", {})
                    for resource, amount in agent_resources.items():
                        actual_used[resource] = actual_used.get(resource, 0) + amount
                
                # 如果实际使用的资源少于记录的，释放多余的部分
                for resource, recorded_amount in used_resources.items():
                    actual_amount = actual_used.get(resource, 0)
                    if recorded_amount > actual_amount:
                        excess = {resource: recorded_amount - actual_amount}
                        logger.debug("释放资源池 %s 中多余的 %s 资源: %s", pool_name, resource, excess)
                        ray.get(self.resource_pools.release_agent_slot.remote(pool_name, excess))

    # ==================================================================
    # Placement Group Pool Management
    # ==================================================================
    
    def configure_pg_pool(self, pool_name: str, **kwargs) -> dict:
        """
        Configure the Placement Group pool for a resource pool.
        
        Args:
            pool_name (str): The name of the resource pool.
            **kwargs: Configuration for the PG pool, passed to PlacementGroupPoolManager.
        
        Returns:
            dict: The result from the PG pool manager.
        """
        if self.pg_pool_manager is None:
            return {"success": False, "error": "PlacementGroupPoolManager not available."}
        
        return ray.get(self.pg_pool_manager.configure_pool.remote(pool_name, **kwargs))
    
    def get_pg_pool_stats(self, pool_name: Optional[str] = None) -> dict:
        """
        Get statistics for the Placement Group pool.
        
        Args:
            pool_name (str, optional): The name of the pool. If None, stats for all pools are returned.
        
        Returns:
            dict: Statistics of the PG pool.
        """
        if self.pg_pool_manager is None:
            return {"success": False, "error": "PlacementGroupPoolManager not available."}
        
        return ray.get(self.pg_pool_manager.get_pool_stats.remote(pool_name))

    # ==================================================================
    # Resource Pool Operations
    # ==================================================================

    def create_resource_pool(
        self,
        name: str,
        labels: dict[str, str],
        resources: dict[str, float],
        target_agents: int = 0,
        placement_strategy: str = "STRICT_PACK",
        pg_pool_config: Optional[dict] = None,
    ) -> dict:
        """
        Create a new resource pool.
        
        Also configures the PG pool if pg_pool_config is provided.
        """
        if self.resource_pools is None:
            return {"success": False, "error": "ResourcePoolManager not available"}
        
        result = ray.get(
            self.resource_pools.create_pool.remote(
                name, labels, resources, target_agents, placement_strategy
            )
        )
        
        # If pool created successfully, configure the PG pool
        if result.get("success") and pg_pool_config and pg_pool_config.get("enable"):
            logger.info(f"Configuring PG pool for new resource pool '{name}'")
            # Remove 'enable' from the config before passing to configure_pool
            pg_config_copy = dict(pg_pool_config)
            pg_config_copy.pop("enable", None)
            pg_config_result = self.configure_pg_pool(name, **pg_config_copy)
            result["pg_pool_config_result"] = pg_config_result
        
        return result

    def create_agent(
        self,
        name: str,
        metadata: dict[str, str],
        actor_class: type,
        resources_override: Optional[dict[str, float]] = None,
        ray_options: Optional[dict[str, Any]] = None,
        actor_args: Optional[list[Any]] = None,
        actor_kwargs: Optional[dict[str, Any]] = None,
    ) -> dict:
        if self.agent_manager is None:
            raise RuntimeError("AgentManagerActor not initialised")
        if self.resource_pools is None:
            raise RuntimeError("ResourcePoolManagerActor not initialised")

        pool_name = metadata.get("pool")
        if not pool_name:
            return {"success": False, "error": "Agent metadata missing 'pool' assignment"}

        logger.info("调度器创建Agent name=%s pool=%s", name, pool_name)

        pool_snapshot = ray.get(self.resource_pools.get_pool.remote(pool_name))
        if pool_snapshot is None:
            logger.warning("Agent创建失败 name=%s pool=%s error=资源池不存在", name, pool_name)
            return {"success": False, "error": f"Pool '{pool_name}' not found"}

        default_resources = pool_snapshot.get("default_agent_resources") or {}
        agent_resources = resources_override or default_resources
        if not agent_resources:
            logger.warning("Agent创建失败 name=%s pool=%s error=未提供资源配置", name, pool_name)
            return {"success": False, "error": "Agent resources must be provided"}

        reservation = ray.get(self.resource_pools.reserve_agent_slot.remote(pool_name, agent_resources))
        if not reservation.get("success"):
            logger.warning(
                "Agent预留失败 name=%s pool=%s error=%s",
                name,
                pool_name,
                reservation.get("error"),
            )
            return reservation

        try:
            manager_kwargs: dict[str, Any] = {"resources": agent_resources}
            if ray_options is not None:
                manager_kwargs["ray_options"] = ray_options
            if actor_args is not None:
                manager_kwargs["actor_args"] = actor_args
            if actor_kwargs is not None:
                manager_kwargs["actor_kwargs"] = actor_kwargs

            response = ray.get(
                self.agent_manager.create_agent.remote(
                    name,
                    dict(metadata),
                    actor_class,
                    **manager_kwargs,
                )
            )
        except Exception:
            # Rollback reservation and propagate error
            ray.get(self.resource_pools.cancel_agent_reservation.remote(pool_name, agent_resources))
            logger.exception("Agent创建抛异常 name=%s pool=%s", name, pool_name)
            raise

        if not response.get("success"):
            ray.get(self.resource_pools.cancel_agent_reservation.remote(pool_name, agent_resources))
            logger.warning(
                "Agent创建失败 name=%s pool=%s error=%s",
                name,
                pool_name,
                response.get("error"),
            )
            return response

        commit = ray.get(self.resource_pools.commit_agent_slot.remote(pool_name, agent_resources))
        if not commit.get("success"):
            # Commit 失败，需要回滚：1) 取消预留 2) 删除已创建的 Agent
            ray.get(self.resource_pools.cancel_agent_reservation.remote(pool_name, agent_resources))
            ray.get(self.agent_manager.delete_agent.remote(name, force=True))
            logger.warning(
                "Agent提交失败 name=%s pool=%s error=%s，已清理Agent",
                name,
                pool_name,
                commit.get("error"),
            )
            return commit

        if self.scheduler is not None:
            # 从AgentManager获取完整的agent信息（包含handle）
            agent_with_handle = ray.get(self.agent_manager.get_agent.remote(name, include_handle=True))
            if agent_with_handle:
                ray.get(self.scheduler.register_agent.remote(agent_with_handle))

        response["agent"]["resources"] = agent_resources
        logger.info(
            "Agent创建成功 name=%s pool=%s 当前占用=%s 资源=%s",
            name,
            pool_name,
            commit["pool"]["used_resources"],
            agent_resources,
        )
        return response

    def delete_agent(self, name: str, *, force: bool = False, destroy_pg: bool = True) -> dict:
        if self.agent_manager is None:
            raise RuntimeError("AgentManagerActor not initialised")
        if self.resource_pools is None:
            raise RuntimeError("ResourcePoolManagerActor not initialised")

        agent_info = ray.get(self.agent_manager.get_agent.remote(name))
        if agent_info is None:
            if force:
                logger.warning("删除Agent时未找到记录 name=%s，忽略", name)
                return {"success": True}
            logger.warning("删除Agent失败 name=%s error=未找到", name)
            return {"success": False, "error": f"Agent '{name}' not found"}

        pool_name = agent_info.get("labels", {}).get("pool")
        resources = agent_info.get("resources", {})

        logger.info("调度器删除Agent name=%s pool=%s", name, pool_name)
        deletion = ray.get(self.agent_manager.delete_agent.remote(name, force=force, destroy_pg=destroy_pg))
        if not deletion.get("success"):
            logger.warning("删除Agent失败 name=%s error=%s", name, deletion.get("error"))
            return deletion

        if pool_name and resources:
            release = ray.get(self.resource_pools.release_agent_slot.remote(pool_name, resources))
            if not release.get("success"):
                logger.warning(
                    "释放资源失败 name=%s pool=%s error=%s",
                    name,
                    pool_name,
                    release.get("error"),
                )
                deletion["warning"] = release.get("error")

        if self.scheduler is not None:
            ray.get(self.scheduler.unregister_agent.remote(name))

        return deletion

    def list_agents(self, pool_name: Optional[str] = None, *, include_handle: bool = False) -> dict:
        """
        获取所有 Agent 信息
        
        Args:
            pool_name: 可选的资源池名称过滤
            include_handle: 是否包含Ray actor句柄（默认False，用于内部调用时设为True）
        
        Returns:
            {"success": bool, "agents": List[dict]} Agent列表
        """
        if self.agent_manager is None:
            raise RuntimeError("AgentManagerActor not initialised")
        agents = ray.get(self.agent_manager.list_agents.remote(include_handle=include_handle))
        if pool_name:
            agents = [agent for agent in agents if agent.get("labels", {}).get("pool") == pool_name]
        return {"success": True, "agents": agents}

    def list_pools(self) -> dict:
        if self.resource_pools is None:
            raise RuntimeError("ResourcePoolManagerActor not initialised")
        snapshot = ray.get(self.resource_pools.snapshot_state.remote())
        return {"success": True, "pools": snapshot.get("pools", [])}

    def get_pool(self, name: str) -> dict:
        if self.resource_pools is None:
            raise RuntimeError("ResourcePoolManagerActor not initialised")
        pool = ray.get(self.resource_pools.get_pool.remote(name))
        if pool is None:
            return {"success": False, "error": f"Pool '{name}' not found"}
        return {"success": True, "pool": pool}

    # Scheduling operations --------------------------------------------------

    def submit_task(
        self,
        label: str,
        payload: Any,
        labels: Optional[dict[str, str]] = None,
        strategy: Optional[str] = None,
        priority: float = 0.0,
        task_id: Optional[str] = None,
    ) -> dict:
        if self.scheduler is None:
            raise RuntimeError("SchedulerActor not initialised")
        logger.debug(
            "调度器提交任务 label=%s payload=%s labels=%s strategy=%s priority=%.2f",
            label, payload, labels, strategy, priority
        )
        return ray.get(self.scheduler.submit_task.remote(label, payload, labels, strategy, priority, task_id))

    def choose_task(self, label: str) -> dict:
        if self.scheduler is None:
            raise RuntimeError("SchedulerActor not initialised")
        result = ray.get(self.scheduler.choose_task.remote(label))
        logger.debug("调度器选择任务 label=%s -> %s", label, result)
        return result

    def update_agent_metrics(self, name: str, metrics: Dict[str, Any]) -> dict:
        if self.scheduler is None:
            raise RuntimeError("SchedulerActor not initialised")
        return ray.get(self.scheduler.update_agent_metrics.remote(name, metrics))
    
    # Preemption operations --------------------------------------------------
    
    def evaluate_preemption(
        self,
        incoming_task_priority: float,
        incoming_task_pool: str,
        incoming_task_labels: Optional[dict[str, str]] = None,
        incoming_task_resources: Optional[dict[str, float]] = None,
    ) -> dict:
        """评估抢占候选"""
        if self.preemption is None:
            raise RuntimeError("PreemptionControllerActor not initialised")
        
        # 将资源字典转换为ResourceSpec对象
        from schedulemesh.core.entities.resource_pool import ResourceSpec
        resource_spec = ResourceSpec.from_dict(incoming_task_resources) if incoming_task_resources else None
        
        return ray.get(
            self.preemption.evaluate_preemption.remote(
                incoming_task_priority=incoming_task_priority,
                incoming_task_pool=incoming_task_pool,
                incoming_task_resources=resource_spec,
                incoming_task_labels=incoming_task_labels,
                agent_manager_handle=self.agent_manager,
                resource_pool_manager_handle=self.resource_pools,
            )
        )
    
    def execute_preemption(
        self, 
        task_id: str, 
        agent_name: Optional[str] = None,
        preemptor_task_id: Optional[str] = None,
    ) -> dict:
        """执行抢占操作"""
        if self.preemption is None:
            raise RuntimeError("PreemptionControllerActor not initialised")
        
        agent_handle = None
        if agent_name and self.agent_manager:
            agent_info = ray.get(self.agent_manager.get_agent.remote(agent_name))
            if agent_info:
                agent_handle = agent_info.get("handle")
        
        return ray.get(self.preemption.execute_preemption.remote(
            task_id, agent_handle, preemptor_task_id
        ))
    
    def register_running_task(
        self,
        task_id: str,
        agent_name: str,
        pool_name: str,
        priority: float,
        labels: Optional[dict[str, str]] = None,
        estimated_duration: float = 0.0,
        payload: Any = None,
        resources: Optional[dict[str, float]] = None,
    ) -> dict:
        """注册运行中的任务"""
        if self.preemption is None:
            raise RuntimeError("PreemptionControllerActor not initialised")
        
        # 如果没有提供资源信息，尝试从Pool获取默认资源
        if resources is None and self.resource_pools is not None:
            try:
                pool_info = ray.get(self.resource_pools.get_pool.remote(pool_name))
                if pool_info and "default_agent_resources" in pool_info and pool_info["default_agent_resources"]:
                    resources = pool_info["default_agent_resources"]
                    logger.debug(
                        "从Pool获取默认资源用于任务注册 task_id=%s pool=%s resources=%s",
                        task_id,
                        pool_name,
                        resources,
                    )
            except Exception as exc:
                logger.warning(
                    "获取Pool默认资源失败 task_id=%s pool=%s error=%s",
                    task_id,
                    pool_name,
                    exc,
                )
        
        # 将资源字典转换为ResourceSpec对象
        from schedulemesh.core.entities.resource_pool import ResourceSpec
        resource_spec = ResourceSpec.from_dict(resources) if resources else None
        
        return ray.get(
            self.preemption.register_running_task.remote(
                task_id,
                agent_name,
                pool_name,
                priority,
                labels,
                estimated_duration,
                payload,
                resource_spec,
            )
        )
    
    def unregister_task(self, task_id: str) -> dict:
        """任务完成，取消注册"""
        if self.preemption is None:
            raise RuntimeError("PreemptionControllerActor not initialised")
        return ray.get(self.preemption.unregister_task.remote(task_id))
    
    def get_preemption_stats(self) -> dict:
        """获取抢占统计"""
        if self.preemption is None:
            raise RuntimeError("PreemptionControllerActor not initialised")
        return ray.get(self.preemption.get_preemption_stats.remote())
    
    def update_preemption_policy(self, **kwargs) -> dict:
        """更新抢占策略"""
        if self.preemption is None:
            raise RuntimeError("PreemptionControllerActor not initialised")
        return ray.get(self.preemption.update_policy.remote(**kwargs))

    def preempt_task(
        self,
        incoming_task_priority: float,
        incoming_task_pool: str,
        incoming_task_labels: Optional[dict[str, str]] = None,
        incoming_task_resources: Optional[dict[str, float]] = None,
        target_task_id: Optional[str] = None,
        target_agent_name: Optional[str] = None,
        incoming_task_id: Optional[str] = None,
    ) -> dict:
        """一站式触发抢占：评估候选并执行"""
        if self.preemption is None:
            raise RuntimeError("PreemptionControllerActor not initialised")

        # 将资源字典转换为ResourceSpec对象
        from schedulemesh.core.entities.resource_pool import ResourceSpec
        resource_spec = ResourceSpec.from_dict(incoming_task_resources) if incoming_task_resources else None

        evaluation = ray.get(
            self.preemption.evaluate_preemption.remote(
                incoming_task_priority=incoming_task_priority,
                incoming_task_pool=incoming_task_pool,
                incoming_task_resources=resource_spec,
                incoming_task_labels=incoming_task_labels,
                agent_manager_handle=self.agent_manager,
                resource_pool_manager_handle=self.resource_pools,
            )
        )
        candidates = list(evaluation.get("candidates") or [])

        if not evaluation.get("should_preempt") or not candidates:
            return {
                "success": False,
                "reason": evaluation.get("reason") or "No preemption candidates",
                "evaluation": evaluation,
            }

        chosen: Optional[Dict[str, Any]] = None
        if target_task_id or target_agent_name:
            for candidate in candidates:
                matches_task = not target_task_id or candidate.get("task_id") == target_task_id
                matches_agent = not target_agent_name or candidate.get("agent_name") == target_agent_name
                if matches_task and matches_agent:
                    chosen = candidate
                    break
            if chosen is None:
                reason_parts = []
                if target_task_id:
                    reason_parts.append(f"task '{target_task_id}'")
                if target_agent_name:
                    reason_parts.append(f"agent '{target_agent_name}'")
                target_desc = " & ".join(reason_parts) or "specified target"
                return {
                    "success": False,
                    "reason": f"{target_desc} not eligible for preemption",
                    "evaluation": evaluation,
                }
        else:
            chosen = candidates[0]

        exec_result = self.execute_preemption(
            chosen.get("task_id"),
            chosen.get("agent_name"),
            preemptor_task_id=incoming_task_id,
        )
        response = {
            "success": exec_result.get("success", False),
            "evaluation": evaluation,
            "chosen_candidate": chosen,
            "execution": exec_result,
        }
        if not response["success"]:
            response["reason"] = exec_result.get("error") or "Preemption execution failed"
        return response

    def register_state_handlers(
        self,
        pool_name: str,
        preserver: Any,
        restorer: Any,
    ) -> dict:
        if self.preemption is None:
            raise RuntimeError("PreemptionControllerActor not initialised")
        return ray.get(
            self.preemption.register_state_handlers.remote(
                pool_name,
                preserver,
                restorer,
            )
        )

    def restore_task(self, saved_state: Dict[str, Any]) -> dict:
        if self.preemption is None:
            raise RuntimeError("PreemptionControllerActor not initialised")
        return ray.get(self.preemption.restore_task.remote(saved_state))

    def detect_stale_agents(self, timeout: float) -> dict:
        if self.scheduler is None:
            raise RuntimeError("SchedulerActor not initialised")
        return ray.get(self.scheduler.detect_stale_agents.remote(timeout))

    def _import_actor_class(self, dotted_path: str):
        from importlib import import_module
        try:
            module_path, _, attr = dotted_path.rpartition(".")
            module = import_module(module_path)
            return getattr(module, attr)
        except Exception:  # pragma: no cover - defensive
            logger.exception("加载自定义 Agent 类失败 path=%s，回退到默认 AgentActor", dotted_path)
            from schedulemesh.core.agent_actor import AgentActor  # pylint: disable=import-outside-toplevel

            return AgentActor

    def reconcile_agents(self, timeout: float = 120.0, recreate: bool = True) -> dict:
        if self.scheduler is None:
            raise RuntimeError("SchedulerActor not initialised")
        if self.agent_manager is None:
            raise RuntimeError("AgentManagerActor not initialised")
        if self.resource_pools is None:
            raise RuntimeError("ResourcePoolManagerActor not initialised")

        health = ray.get(self.scheduler.detect_stale_agents.remote(timeout))
        timestamp = health.get("timestamp", 0.0) or time.time()
        recovered: list[str] = []
        recreated: list[str] = []

        stale_entries = {agent.get("name"): agent for agent in (health.get("stale") or []) if agent.get("name")}
        combined_entries = {agent.get("name"): agent for agent in (health.get("active") or []) if agent.get("name")}
        combined_entries.update(stale_entries)

        processed: set[str] = set()
        for name, snapshot in combined_entries.items():
            if not name or name in processed:
                continue
            processed.add(name)

            agent_info = ray.get(self.agent_manager.get_agent.remote(name))
            is_stale = False
            if name in stale_entries:
                is_stale = True
            if agent_info is None:
                is_stale = True
            last = snapshot.get("last_heartbeat") or 0.0
            if timeout > 0 and (not last or timestamp - last > timeout):
                is_stale = True

            if not is_stale:
                continue

            if agent_info is None:
                agent_info = snapshot
            else:
                recovered.append(name)

            pool_name = (agent_info.get("labels") or {}).get("pool")
            resources = dict(agent_info.get("resources") or {})
            actor_class_path = agent_info.get("actor_class_path")
            actor_args = agent_info.get("actor_init_args") or []
            actor_kwargs = agent_info.get("actor_init_kwargs") or {}
            ray_options = agent_info.get("options") or {}

            # 删除旧 Agent（若仍存在）
            deletion = self.delete_agent(name, force=True)
            if not deletion.get("success"):
                logger.debug("删除 Agent %s 失败或已不存在: %s", name, deletion.get("error"))

            if not recreate or not pool_name:
                continue

            pool_snapshot = ray.get(self.resource_pools.get_pool.remote(pool_name))
            if pool_snapshot is None:
                logger.debug("资源池 %s 不存在，跳过重建 Agent %s", pool_name, name)
                continue

            # 计算当前资源池中真正活跃的 Agent 数量（排除即将被删除的 stale agent）
            active_agents = [
                a
                for a in health.get("active", [])
                if (a.get("labels") or {}).get("pool") == pool_name and a.get("name") not in processed
            ]
            target_agents = pool_snapshot.get("target_agents", 0)
            if target_agents and len(active_agents) >= target_agents:
                continue

            if not resources:
                resources = pool_snapshot.get("default_agent_resources") or pool_snapshot.get("capacity") or {}

            actor_class = self._import_actor_class(actor_class_path) if actor_class_path else None
            if actor_class is None:
                from schedulemesh.core.agent_actor import AgentActor  # pylint: disable=import-outside-toplevel

                actor_class = AgentActor

            new_name = name
            if ray.get(self.agent_manager.get_agent.remote(new_name)) is not None:
                import uuid

                new_name = f"{pool_name}-agent-{uuid.uuid4().hex[:6]}"

            create_result = self.create_agent(
                new_name,
                {"pool": pool_name},
                actor_class,
                resources_override=resources,
                ray_options=ray_options,
                actor_args=actor_args,
                actor_kwargs=actor_kwargs,
            )
            if create_result.get("success"):
                recreated.append(new_name)
            else:
                logger.warning("重建 Agent %s 失败: %s", new_name, create_result.get("error"))

        return {
            "stale": list(processed),
            "recovered": recovered,
            "recreated": recreated,
            "timestamp": health.get("timestamp"),
        }

    def submit_task_with_preemption(self, **kwargs: Any) -> dict:
        """自动化处理任务提交、资源检查和抢占"""
        # 1. 参数校验
        pool_name = kwargs.get("pool_name")
        resources = kwargs.get("resources")
        actor_class = kwargs.get("actor_class")
        task_id = kwargs.get("task_id")
        priority = kwargs.get("priority", 5.0)  # 默认为中等优先级
        labels = kwargs.get("labels", {})
        
        if not all([pool_name, resources, actor_class, task_id]):
            return {"success": False, "error": "Missing required arguments for submit_task"}

        # 确保actor句柄不在kwargs中传递
        kwargs.pop("actor_handle", None)
        
        logger.info("自动化任务提交 task_id=%s pool=%s", task_id, pool_name)

        # 2. 检查资源池是否有足够资源
        # 注意：这里我们使用 reserve_agent_slot 尝试预留资源，这是一个原子操作
        reservation = ray.get(
            self.resource_pools.reserve_agent_slot.remote(pool_name, resources)
        )

        if reservation.get("success"):
            # 资源充足，直接创建 Agent
            logger.info("资源充足，直接创建 Agent task_id=%s", task_id)
            agent_creation_result = self._create_agent_after_reservation(
                reservation, **kwargs
            )
            # 无论创建成功与否，都要释放预留（如果失败的话）
            if not agent_creation_result.get("success"):
                ray.get(self.resource_pools.cancel_agent_reservation.remote(
                    pool_name, resources
                ))
            return agent_creation_result
            
        logger.warning(
            "资源不足 task_id=%s, reason=%s。尝试自动抢占...",
            task_id,
            reservation.get("error"),
        )

        # 3. 资源不足，触发自动抢占
        if self.preemption is None:
            return {"success": False, "error": "Preemption controller not available"}
        
        # 将资源字典转换为ResourceSpec对象
        from schedulemesh.core.entities.resource_pool import ResourceSpec
        incoming_task_resources = ResourceSpec.from_dict(resources)
        
        preempt_eval = ray.get(
            self.preemption.evaluate_preemption.remote(
                incoming_task_priority=priority,
                incoming_task_pool=pool_name,
                incoming_task_resources=incoming_task_resources,
                incoming_task_labels=labels,
                agent_manager_handle=self.agent_manager,
                resource_pool_manager_handle=self.resource_pools,
            )
        )

        if not preempt_eval.get("should_preempt"):
            logger.error("抢占评估失败 task_id=%s, 无可用抢占目标", task_id)
            return {
                "success": False,
                "error": "Insufficient resources and no available preemption candidates",
                "reason": "preemption_failed",
            }

        # 4. 选择最佳抢占目标并执行抢占
        candidates = preempt_eval.get("candidates", [])
        if not candidates:
            logger.error("抢占评估返回空候选列表 task_id=%s", task_id)
            return {
                "success": False,
                "error": "No preemption candidates available",
                "reason": "empty_candidates",
            }
        
        chosen_candidate = candidates[0]
        victim_task_id = chosen_candidate["task_id"]
        victim_agent_name = chosen_candidate["agent_name"]
        
        logger.info(
            "找到抢占目标 task_id=%s, victim_task=%s, score=%.2f",
            task_id,
            victim_task_id,
            chosen_candidate["preempt_score"],
        )
        
        # 获取 victim agent 的句柄以执行取消操作（include_handle=True）
        victim_agent_info = ray.get(self.agent_manager.get_agent.remote(victim_agent_name, include_handle=True))
        victim_agent_handle = (
            victim_agent_info.get("handle") if victim_agent_info else None
        )

        execution_result = ray.get(
            self.preemption.execute_preemption.remote(
                task_id=victim_task_id, 
                agent_handle=victim_agent_handle,
                preemptor_task_id=task_id,
            )
        )
        
        if not execution_result.get("success"):
            logger.error("执行抢占失败 task_id=%s, victim_task=%s", task_id, victim_task_id)
            return {
                "success": False,
                "error": f"Failed to execute preemption on task {victim_task_id}",
                "reason": "preemption_execution_failed",
            }
        
        # 抢占后，释放被抢占任务所占用的资源
        # 删除被抢占的 Agent，释放底层资源
        if victim_agent_name:
            deletion_result = self.delete_agent(victim_agent_name, force=True)
            if not deletion_result.get("success"):
                logger.warning(
                    "自动抢占后删除 Agent 失败 task_id=%s agent=%s error=%s",
                    task_id,
                    victim_agent_name,
                    deletion_result.get("error"),
                )

        logger.info(
            "抢占成功 task_id=%s, victim_task=%s 已释放资源。再次尝试创建 Agent...",
            task_id,
            victim_task_id,
        )

        # 5. 再次尝试预留资源并创建 Agent
        final_reservation = ray.get(
            self.resource_pools.reserve_agent_slot.remote(pool_name, resources)
        )
        
        if not final_reservation.get("success"):
            # 这种情况理论上不应发生，除非有竞态条件
            logger.error("抢占后资源预留仍然失败 task_id=%s", task_id)
            return {
                "success": False,
                "error": "Failed to reserve resources even after preemption",
                "reason": "post_preemption_reservation_failed",
            }
        
        final_creation_result = self._create_agent_after_reservation(
            final_reservation, **kwargs
        )
        if not final_creation_result.get("success"):
            ray.get(self.resource_pools.cancel_agent_reservation.remote(
                pool_name, resources
            ))

        return final_creation_result

    def submit_task_with_pg_preemption(self, **kwargs: Any) -> dict:
        """
        使用 PG 池自动化处理任务提交、资源检查和抢占。
        这是推荐的、用于实现快速抢占的新方法。
        """
        # 1. 参数校验
        pool_name = kwargs.get("pool_name")
        resources = kwargs.get("resources")
        actor_class = kwargs.get("actor_class")
        task_id = kwargs.get("task_id")
        priority = kwargs.get("priority", 5.0)
        labels = kwargs.get("labels", {})

        if not all([pool_name, resources, actor_class, task_id]):
            return {"success": False, "error": "Missing required arguments for submit_task_with_pg_preemption"}

        logger.info(f"PG-based task submission started: task_id={task_id} pool={pool_name} priority={priority}")

        # 2. 尝试直接从 PG 池创建 Agent
        agent_creation_result = ray.get(
            self.agent_manager.create_agent_with_pg.remote(
                name=task_id,  # Use task_id as agent name for simplicity
                pool_name=pool_name,
                actor_class=actor_class,
                resources=resources,
                priority=priority,
                labels=labels,
                actor_args=kwargs.get("actor_args"),
                actor_kwargs=kwargs.get("actor_kwargs"),
                ray_options=kwargs.get("ray_options"),
            )
        )

        if agent_creation_result.get("success"):
            # PG 创建成功后，必须完成资源池管理和调度器注册
            logger.info(f"Task {task_id} successfully created using PG pool. 正在完成资源提交和注册...")
            
            # 1. 预留资源池配额（PG 已经保证了物理资源，这里管理虚拟配额）
            reservation = ray.get(
                self.resource_pools.reserve_agent_slot.remote(pool_name, resources)
            )
            
            if not reservation.get("success"):
                logger.error(f"PG Agent {task_id} 创建成功但资源池预留失败: {reservation.get('error')}. 清理Agent...")
                # 清理已创建的 Agent
                ray.get(self.agent_manager.delete_agent.remote(task_id, force=True, destroy_pg=False))
                return {"success": False, "error": f"Resource pool reservation failed: {reservation.get('error')}"}
            
            # 2. 提交资源使用
            try:
                commit_result = ray.get(self.resource_pools.commit_agent_slot.remote(pool_name, resources))
                if not commit_result.get("success"):
                    logger.error(f"Task {task_id}: 资源池提交失败: {commit_result.get('error')}. 取消预留并清理Agent...")
                    ray.get(self.resource_pools.cancel_agent_reservation.remote(pool_name, resources))
                    ray.get(self.agent_manager.delete_agent.remote(task_id, force=True, destroy_pg=False))
                    return {"success": False, "error": f"Resource pool commit failed: {commit_result.get('error')}"}
                logger.debug(f"Task {task_id}: 资源池配额已提交")
            except Exception as exc:
                logger.error(f"Task {task_id}: 资源池提交异常: {exc}. 取消预留并清理Agent...")
                ray.get(self.resource_pools.cancel_agent_reservation.remote(pool_name, resources))
                ray.get(self.agent_manager.delete_agent.remote(task_id, force=True, destroy_pg=False))
                return {"success": False, "error": f"Resource pool commit exception: {exc}"}
            
            # 3. 注册到调度器（使用带handle的agent信息）
            if self.scheduler is not None:
                try:
                    agent_with_handle = ray.get(self.agent_manager.get_agent.remote(task_id, include_handle=True))
                    if agent_with_handle:
                        ray.get(self.scheduler.register_agent.remote(agent_with_handle))
                        logger.debug(f"Task {task_id}: 已注册到调度器")
                    else:
                        logger.warning(f"Task {task_id}: 无法获取Agent信息进行调度器注册")
                except Exception as exc:
                    logger.warning(f"Task {task_id}: 调度器注册失败: {exc}")
            
            # 4. 如果需要，注册运行任务到抢占控制器
            if self.preemption is not None and kwargs.get("auto_register", True):
                try:
                    # 调用 supervisor 的 register_running_task，它会处理类型转换
                    self.register_running_task(
                        task_id=task_id,
                        agent_name=task_id,
                        pool_name=pool_name,
                        priority=priority,
                        labels=labels,
                        estimated_duration=kwargs.get("estimated_duration", 0.0),
                        resources=resources,  # 传递原始 dict
                    )
                    logger.debug(f"Task {task_id}: 已注册到抢占控制器")
                except Exception as exc:
                    logger.warning(f"Task {task_id}: 抢占控制器注册失败: {exc}")
            
            logger.info(f"Task {task_id}: PG流程完成，系统状态一致")
            return agent_creation_result

        logger.warning(
            f"Failed to create agent for task {task_id} directly from PG pool: {agent_creation_result.get('error')}. "
            "Attempting preemption..."
        )

        # 3. PG 池资源不足，触发抢占
        if self.preemption is None:
            return {"success": False, "error": "Preemption controller not available"}

        # 评估可抢占的候选者
        from schedulemesh.core.entities.resource_pool import ResourceSpec
        resources_spec = ResourceSpec.from_dict(resources)
        eval_result = ray.get(
            self.preemption.evaluate_preemption.remote(
                incoming_task_priority=priority,
                incoming_task_pool=pool_name,
                incoming_task_labels=labels,
                incoming_task_resources=resources_spec,
                agent_manager_handle=self.agent_manager,
            )
        )

        if not eval_result.get("should_preempt"):
            return {"success": False, "error": "No suitable preemption candidates found."}

        # 选择最佳候选者执行抢占 (选择第一个，因为它们已按分数排序)
        candidate = eval_result["candidates"][0]
        victim_agent_name = candidate["agent_name"]
        victim_task_id = candidate["task_id"]
        
        logger.info(
            f"执行 PG 感知抢占: Task {task_id} will preempt Task {victim_task_id} (Agent: {victim_agent_name})"
        )

        # 第一步：执行任务级抢占（调用 PreemptionController）
        # 这会处理：任务cancel、状态保存、历史记录、metrics更新、从running_tasks移除
        victim_agent_info = ray.get(self.agent_manager.get_agent.remote(victim_agent_name, include_handle=True))
        victim_agent_handle = victim_agent_info.get("handle") if victim_agent_info else None
        
        execution_result = ray.get(
            self.preemption.execute_preemption.remote(
                task_id=victim_task_id, 
                agent_handle=victim_agent_handle,
                preemptor_task_id=task_id,
            )
        )
        
        if not execution_result.get("success"):
            logger.error("PG 抢占：任务级抢占失败 task_id=%s, victim_task=%s", task_id, victim_task_id)
            return {
                "success": False,
                "error": f"Failed to execute preemption on task {victim_task_id}",
                "reason": "preemption_execution_failed",
            }
        
        # 第二步：删除 Agent 但保留其 PG 以便快速复用
        # 注意：这会调用 scheduler.unregister_agent 清理注册表，并通过 destroy_pg=False 保留 PG 供复用
        deletion_result = self.delete_agent(victim_agent_name, force=True, destroy_pg=False)
        if not deletion_result.get("success"):
            logger.warning(
                "PG 抢占：Agent 删除失败，但任务级抢占已完成 task_id=%s agent=%s error=%s",
                task_id,
                victim_agent_name,
                deletion_result.get("error"),
            )
        
        logger.info(
            "PG 感知抢占完成 task_id=%s, victim_task=%s latency=%.2fms", 
            task_id, 
            victim_task_id,
            execution_result.get("execution_latency_ms", 0)
        )

        # 4. 再次尝试使用（现在可用的）PG 池创建 Agent
        # 这次应该会成功，因为它会复用刚刚释放的 PG
        retry_result = ray.get(
            self.agent_manager.create_agent_with_pg.remote(
                name=task_id,
                pool_name=pool_name,
                actor_class=actor_class,
                resources=resources,
                priority=priority,
                labels=labels,
                actor_args=kwargs.get("actor_args"),
                actor_kwargs=kwargs.get("actor_kwargs"),
                ray_options=kwargs.get("ray_options"),
            )
        )
        
        if retry_result.get("success"):
            logger.info(f"Task {task_id} successfully created after preemption. 正在完成资源提交和注册...")
            
            # 抢占后的Agent创建同样需要完成资源管理和注册
            # 1. 预留资源池配额（抢占已释放资源，此时应该能成功）
            reservation = ray.get(
                self.resource_pools.reserve_agent_slot.remote(pool_name, resources)
            )
            
            if not reservation.get("success"):
                logger.error(f"抢占后Agent {task_id} 资源池预留失败: {reservation.get('error')}. 清理Agent...")
                ray.get(self.agent_manager.delete_agent.remote(task_id, force=True, destroy_pg=False))
                return {"success": False, "error": f"Post-preemption resource reservation failed: {reservation.get('error')}"}
            
            # 2. 提交资源使用
            try:
                commit_result = ray.get(self.resource_pools.commit_agent_slot.remote(pool_name, resources))
                if not commit_result.get("success"):
                    logger.error(f"抢占后Task {task_id}: 资源池提交失败: {commit_result.get('error')}. 取消预留并清理Agent...")
                    ray.get(self.resource_pools.cancel_agent_reservation.remote(pool_name, resources))
                    ray.get(self.agent_manager.delete_agent.remote(task_id, force=True, destroy_pg=False))
                    return {"success": False, "error": f"Post-preemption resource commit failed: {commit_result.get('error')}"}
                logger.debug(f"抢占后Task {task_id}: 资源池配额已提交")
            except Exception as exc:
                logger.error(f"抢占后Task {task_id}: 资源池提交异常: {exc}. 取消预留并清理Agent...")
                ray.get(self.resource_pools.cancel_agent_reservation.remote(pool_name, resources))
                ray.get(self.agent_manager.delete_agent.remote(task_id, force=True, destroy_pg=False))
                return {"success": False, "error": f"Post-preemption resource commit exception: {exc}"}
            
            # 3. 注册到调度器
            if self.scheduler is not None:
                try:
                    agent_with_handle = ray.get(self.agent_manager.get_agent.remote(task_id, include_handle=True))
                    if agent_with_handle:
                        ray.get(self.scheduler.register_agent.remote(agent_with_handle))
                        logger.debug(f"抢占后Task {task_id}: 已注册到调度器")
                    else:
                        logger.warning(f"抢占后Task {task_id}: 无法获取Agent信息进行调度器注册")
                except Exception as exc:
                    logger.warning(f"抢占后Task {task_id}: 调度器注册失败: {exc}")
            
            # 4. 注册运行任务到抢占控制器
            if self.preemption is not None and kwargs.get("auto_register", True):
                try:
                    # 调用 supervisor 的 register_running_task，它会处理类型转换
                    self.register_running_task(
                        task_id=task_id,
                        agent_name=task_id,
                        pool_name=pool_name,
                        priority=priority,
                        labels=labels,
                        estimated_duration=kwargs.get("estimated_duration", 0.0),
                        resources=resources,  # 传递原始 dict
                    )
                    logger.debug(f"抢占后Task {task_id}: 已注册到抢占控制器")
                except Exception as exc:
                    logger.warning(f"抢占后Task {task_id}: 抢占控制器注册失败: {exc}")
            
            logger.info(f"抢占后Task {task_id}: 完成，系统状态一致")
        else:
            logger.error(f"FATAL: Failed to create agent for task {task_id} even after preemption: {retry_result.get('error')}")

        return retry_result

    def _create_agent_after_reservation(
        self, reservation: dict, **kwargs: Any
    ) -> dict:
        """在成功预留资源后创建 Agent 的辅助方法"""
        pool_name = kwargs["pool_name"]
        resources = kwargs["resources"]
        
        # 从 kwargs 提取 create_agent 所需的参数
        agent_name = kwargs.get("agent_name") or f"{pool_name}-agent-{uuid.uuid4().hex[:6]}"
        actor_class = kwargs["actor_class"]
        
        # 构建标签，包含 pool 信息
        labels = dict(kwargs.get("labels") or {})
        labels["pool"] = pool_name
        actor_args = kwargs.get("actor_args")
        actor_kwargs = kwargs.get("actor_kwargs")
        
        # 调用 create_agent 方法
        manager_kwargs: dict[str, Any] = {"resources": resources}
        ray_opts = kwargs.get("ray_options")
        if ray_opts is not None:
            manager_kwargs["ray_options"] = ray_opts
        if actor_args is not None:
            manager_kwargs["actor_args"] = actor_args
        if actor_kwargs is not None:
            manager_kwargs["actor_kwargs"] = actor_kwargs

        creation_result = ray.get(
            self.agent_manager.create_agent.remote(
                agent_name,
                labels,
                actor_class,
                **manager_kwargs,
            )
        )

        if creation_result.get("success"):
            # 创建成功，提交资源占用
            commit_result = ray.get(self.resource_pools.commit_agent_slot.remote(
                pool_name, resources
            ))
            
            if not commit_result.get("success"):
                # Commit 失败，需要：1) 取消预留 2) 删除已创建的 Agent
                logger.error(
                    "Agent 创建成功但资源池提交失败 task_id=%s, name=%s, error=%s. 取消预留并清理Agent...",
                    kwargs["task_id"],
                    agent_name,
                    commit_result.get("error")
                )
                # 1. 取消资源预留（否则资源会永久占用在 reserved 状态）
                ray.get(self.resource_pools.cancel_agent_reservation.remote(pool_name, resources))
                # 2. 删除已创建的 Agent
                ray.get(self.agent_manager.delete_agent.remote(agent_name, force=True))
                return {
                    "success": False, 
                    "error": f"Resource pool commit failed: {commit_result.get('error')}"
                }
            
            logger.info("Agent 创建成功 task_id=%s, name=%s", kwargs["task_id"], agent_name)
        else:
            logger.error(
                "Agent 创建失败 task_id=%s, error=%s",
                kwargs["task_id"],
                creation_result.get("error"),
            )
            # 创建失败，不需要取消预留，因为上层调用会处理

        return creation_result
