"""
AgentManager actor skeleton.

Responsible for creating and supervising worker agents.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import ray
from ray.exceptions import RayActorError

from .config import ActorConfig
from schedulemesh.core.entities.resource_pool import ResourceSpec
from schedulemesh.core.entities.types import AgentStatus
from schedulemesh.core.utils import configure_runtime_logging

logger = logging.getLogger(__name__)


@ray.remote
class AgentManagerActor:
    """Placeholder implementation for an agent lifecycle manager."""

    def __init__(self, config: ActorConfig, pg_pool_manager: Optional[ray.actor.ActorHandle] = None):
        configure_runtime_logging()
        self.config = config
        self.agents: Dict[str, dict] = {}
        self.pg_pool_manager = pg_pool_manager
        logger.debug("AgentManagerActor[%s] initialised", config.name)

    def _sanitize_agent(self, record: dict, *, include_handle: bool = True) -> dict:
        public_record = record.copy()
        if not include_handle:
            public_record.pop("handle", None)
        public_record.pop("actor_class", None)
        status = public_record.get("status")
        if isinstance(status, AgentStatus):
            public_record["status"] = status.value
        return public_record

    def create_agent(
        self,
        name: str,
        labels: dict[str, str],
        actor_class: type,
        resources: dict[str, float],
        ray_options: Optional[dict[str, Any]] = None,
        actor_args: Optional[List[Any]] = None,
        actor_kwargs: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """Register a new agent and instantiate the Ray actor."""
        if name in self.agents:
            return {"success": False, "error": f"Agent '{name}' already exists"}

        resource_spec = ResourceSpec.from_dict(resources)
        options: dict[str, Any] = dict(ray_options or {})

        # 如果已经指定了 placement_group，则不设置资源选项
        # 因为 PG 已经保证了资源分配，重复声明会导致 Ray 叠加资源需求
        has_pg = "placement_group" in options
        
        if not has_pg:
            # 传统路径：没有 PG，需要设置资源选项
            resource_options = resource_spec.to_ray_options()
            for key, value in resource_options.items():
                if key == "resources":
                    existing_resources = dict(options.get("resources", {}))
                    for res_key, res_value in value.items():
                        existing_resources.setdefault(res_key, res_value)
                    if existing_resources:
                        options["resources"] = existing_resources
                else:
                    options.setdefault(key, value)
        else:
            # PG 路径：资源由 PG 保证，不再追加资源选项
            logger.debug(
                f"Agent {name} 使用 PlacementGroup，跳过资源选项设置以避免重复声明"
            )

        labels_copy = dict(labels)

        logger.debug("AgentManager 创建 Ray actor name=%s options=%s", name, options)
        init_args: List[Any] = [name, labels_copy]
        if actor_args:
            init_args.extend(actor_args)
        init_kwargs: Dict[str, Any] = dict(actor_kwargs or {})
        actor_handle = actor_class.options(**options).remote(*init_args, **init_kwargs)

        actor_metadata = getattr(actor_class, "__ray_metadata__", None)
        if actor_metadata:
            actor_module = getattr(actor_metadata, "module", None) or actor_class.__module__
            actor_name = getattr(actor_metadata, "class_name", None) or getattr(actor_class, "__name__", "AgentActor")
            actor_class_path = f"{actor_module}.{actor_name}"
        else:
            qual = getattr(actor_class, "__qualname__", getattr(actor_class, "__name__", "AgentActor"))
            actor_class_path = f"{actor_class.__module__}.{qual}"

        agent_record = {
            "name": name,
            "labels": labels_copy,
            "resources": resource_spec.to_dict(),
            "actor_class": actor_class,
            "actor_class_path": actor_class_path,
            "actor_init_args": list(actor_args or []),
            "actor_init_kwargs": dict(actor_kwargs or {}),
            "handle": actor_handle,
            "options": options,
            "status": AgentStatus.RUNNING,
        }
        self.agents[name] = agent_record

        logger.info("Agent %s 注册完成", name)
        response_payload = self._sanitize_agent(agent_record)
        response_payload.pop("actor_class", None)
        return {"success": True, "agent": response_payload}

    def create_agent_with_pg(
        self,
        name: str,
        pool_name: str,
        actor_class: type,
        resources: dict,
        *,
        priority: float = 5.0,
        labels: Optional[dict] = None,
        ray_options: Optional[dict] = None,
        actor_args: Optional[List[Any]] = None,
        actor_kwargs: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """Create an agent, using the PG pool to allocate resources."""
        
        # 强制添加 pool 标签，确保 Agent 记录包含所属 pool 信息
        enhanced_labels = dict(labels or {})
        enhanced_labels["pool"] = pool_name
        logger.debug(f"Agent {name}: 强制添加 pool 标签: {enhanced_labels}")
        
        if self.pg_pool_manager is None:
            logger.warning("PG Pool Manager not available, falling back to legacy agent creation.")
            return self.create_agent(
                name, enhanced_labels, actor_class, resources, ray_options, actor_args, actor_kwargs
            )

        # 1. Allocate a PG from the pool
        pg_result = ray.get(
            self.pg_pool_manager.allocate_pg.remote(
                agent_name=name,
                pool_name=pool_name,
                spec_dict=resources,
                priority=priority,
            )
        )

        if not pg_result.get("success"):
            logger.warning(
                f"Failed to allocate PG for Agent {name}, falling back to legacy creation. "
                f"Reason: {pg_result.get('error')}"
            )
            return self.create_agent(
                name, enhanced_labels, actor_class, resources, ray_options, actor_args, actor_kwargs
            )

        # 2. Use the allocated PG to create the agent
        pg_handle = pg_result["pg_handle"]
        options = dict(ray_options or {})
        options["placement_group"] = pg_handle
        options["placement_group_bundle_index"] = 0
        
        # Resources are guaranteed by the PG, so remove them from options to avoid conflicts
        options.pop("num_cpus", None)
        options.pop("num_gpus", None)
        options.pop("memory", None)
        options.pop("resources", None)

        logger.info(
            f"Agent {name} will be created with PG {pg_result['pg_id']} "
            f"(tier={pg_result['priority_tier']})"
        )

        result = self.create_agent(
            name, enhanced_labels, actor_class, resources, options, actor_args, actor_kwargs
        )
        
        # Add PG info to the agent record
        if result.get("success"):
            self.agents[name]["pg_info"] = {
                "pg_id": pg_result["pg_id"],
                "priority_tier": pg_result["priority_tier"],
            }
        else:
            # If agent creation failed, release the PG
            logger.error(f"Agent {name} creation failed after PG allocation. Releasing PG.")
            ray.get(self.pg_pool_manager.release_pg.remote(name, destroy=True))
        
        return result

    def get_agent(self, name: str, *, include_handle: bool = False) -> Optional[dict]:
        """
        获取Agent信息
        
        Args:
            name: Agent名称
            include_handle: 是否包含Ray actor句柄（默认False，用于序列化安全）
        
        Returns:
            Agent信息字典，如果不存在则返回None
        """
        record = self.agents.get(name)
        if record is None:
            return None
        return self._sanitize_agent(record, include_handle=include_handle)

    def list_agents(self, *, include_handle: bool = False) -> list[dict]:
        """
        列出所有Agent
        
        Args:
            include_handle: 是否包含Ray actor句柄（默认False）
        
        Returns:
            Agent信息列表
        """
        return [self._sanitize_agent(record, include_handle=include_handle) for record in self.agents.values()]

    def delete_agent(self, name: str, *, force: bool = False, destroy_pg: bool = True) -> dict:
        record = self.agents.get(name)
        if record is None:
            return {"success": False, "error": f"Agent '{name}' not found"}

        handle = record.get("handle")
        record["status"] = AgentStatus.TERMINATING

        if handle is not None:
            try:
                ray.kill(handle, no_restart=True)
            except RayActorError as exc:
                logger.warning("Agent %s kill raised RayActorError: %s", name, exc)
                if not force:
                    record["status"] = AgentStatus.ERROR
                    return {"success": False, "error": f"Failed to terminate agent '{name}': {exc}"}
            except Exception as exc:
                logger.exception("Agent %s kill failed", name)
                if not force:
                    record["status"] = AgentStatus.ERROR
                    return {"success": False, "error": f"Failed to terminate agent '{name}': {exc}"}
        
        # Release the PG if it exists
        if self.pg_pool_manager and "pg_info" in record:
            pg_info = record["pg_info"]
            logger.info(
                f"Releasing PG {pg_info['pg_id']} for deleted agent {name} "
                f"(priority={pg_info.get('priority_tier')}, destroy={destroy_pg})"
            )
            should_destroy = destroy_pg
            ray.get(
                self.pg_pool_manager.release_pg.remote(
                    agent_name=name, destroy=should_destroy, force=True
                )
            )

        record["status"] = AgentStatus.DELETED
        removed = self.agents.pop(name)
        logger.info("Agent %s 已删除", name)
        public_payload = self._sanitize_agent(removed)
        public_payload.pop("actor_class", None)
        return {"success": True, "agent": public_payload}

    def delete_agent_with_pg(
        self,
        name: str,
        *,
        force: bool = False,
        destroy_pg: bool = True,
    ) -> dict:
        """
        删除 Agent 并管理其 PG
        
        Args:
            name: Agent 名称
            force: 强制删除（即使失败也返回成功）
            destroy_pg: 是否销毁 PG（True=销毁, False=回收到池中复用）
        
        Returns:
            {"success": bool, "agent": dict, "pg_released": bool, "error": str}
        """
        record = self.agents.get(name)
        if record is None:
            if force:
                return {"success": True, "message": f"Agent '{name}' not found (forced)"}
            return {"success": False, "error": f"Agent '{name}' not found"}

        handle = record.get("handle")
        record["status"] = AgentStatus.TERMINATING

        # 终止 Ray actor
        if handle is not None:
            try:
                ray.kill(handle, no_restart=True)
            except RayActorError as exc:
                logger.warning("Agent %s kill raised RayActorError: %s", name, exc)
                if not force:
                    record["status"] = AgentStatus.ERROR
                    return {"success": False, "error": f"Failed to terminate agent '{name}': {exc}"}
            except Exception as exc:
                logger.exception("Agent %s kill failed", name)
                if not force:
                    record["status"] = AgentStatus.ERROR
                    return {"success": False, "error": f"Failed to terminate agent '{name}': {exc}"}
        
        # 释放 PG
        pg_released = False
        if self.pg_pool_manager and "pg_info" in record:
            pg_info = record["pg_info"]
            logger.info(
                f"释放 Agent {name} 的 PG {pg_info['pg_id']} (destroy={destroy_pg})"
            )
            
            # 调用 pg_pool_manager 释放 PG
            release_result = ray.get(
                self.pg_pool_manager.release_pg.remote(
                    agent_name=name,
                    destroy=destroy_pg,
                    force=True,
                )
            )
            
            if release_result.get("success"):
                pg_released = True
                logger.info(
                    f"PG {pg_info['pg_id']} 已{'销毁' if destroy_pg else '释放并可用于复用'}"
                )
            else:
                logger.warning(
                    f"释放 PG {pg_info['pg_id']} 失败: {release_result.get('error')}"
                )

        record["status"] = AgentStatus.DELETED
        removed = self.agents.pop(name)
        logger.info("Agent %s 已删除 (PG released: %s)", name, pg_released)
        
        public_payload = self._sanitize_agent(removed)
        public_payload.pop("actor_class", None)
        return {
            "success": True,
            "agent": public_payload,
            "pg_released": pg_released,
        }
