"""
ResourcePoolManager actor skeleton.

This actor will own the lifecycle of resource pools (brokers) and maintain
label-based indices for scheduling decisions.
"""

from __future__ import annotations

import logging
import json
from pathlib import Path
from typing import Dict, Optional

import ray

from .config import ActorConfig
from schedulemesh.core.entities.resource_pool import ResourcePool, ResourceSpec
from schedulemesh.core.utils import configure_runtime_logging

logger = logging.getLogger(__name__)


@ray.remote
class ResourcePoolManagerActor:
    """Stub implementation for the resource pool management actor."""

    def __init__(self, config: ActorConfig):
        configure_runtime_logging()
        self.config = config
        self.pools: Dict[str, ResourcePool] = {}
        self.cluster_total = ResourceSpec()
        self.cluster_available = ResourceSpec()
        self.cluster_committed = ResourceSpec()
        self._state_path = Path(config.state_path).expanduser() if config.state_path else None
        logger.info("ResourcePoolManagerActor[%s] initialised", config.name)
        self._load_state()

    # ------------------------------------------------------------------
    # Cluster inventory helpers

    def _update_cluster_inventory(self) -> None:
        """Refresh cached view of cluster-wide resources."""
        try:
            total = ResourceSpec()
            for node in ray.nodes():
                total = total + ResourceSpec.from_ray_resources(node.get("Resources", {}))
            available_resources = ray.available_resources()
            available = ResourceSpec.from_ray_resources(available_resources)
        except Exception:  # pragma: no cover - defensive guard for Ray internals
            logger.exception("Failed to refresh cluster inventory")
            total = ResourceSpec()
            available = ResourceSpec()

        self.cluster_total = total
        self.cluster_available = available
        logger.info(
            "集群资源刷新: 总量=%s 可用=%s 已承诺=%s",
            self.cluster_total,
            self.cluster_available,
            self.cluster_committed,
        )

    def _remaining_capacity(self) -> ResourceSpec:
        return self.cluster_total - self.cluster_committed

    def _cluster_snapshot(self) -> dict:
        return {
            "total": self.cluster_total.to_dict(),
            "available": self.cluster_available.to_dict(),
            "committed": self.cluster_committed.to_dict(),
        }

    # ------------------------------------------------------------------
    # Persistence helpers

    def _state_file(self) -> Optional[Path]:
        if self._state_path is None:
            return None
        self._state_path.mkdir(parents=True, exist_ok=True)
        return self._state_path / f"{self.config.name}-resource-pools.json"

    def _save_state(self) -> None:
        state_file = self._state_file()
        if state_file is None:
            return
        payload = {
            "cluster": self._cluster_snapshot(),
            "pools": [pool.to_dict() for pool in self.pools.values()],
        }
        try:
            state_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:  # pragma: no cover - defensive
            logger.exception("资源池状态持久化失败")

    def _load_state(self) -> None:
        state_file = self._state_file()
        if state_file is None or not state_file.exists():
            return
        try:
            data = json.loads(state_file.read_text(encoding="utf-8"))
        except Exception:  # pragma: no cover - defensive
            logger.exception("资源池状态恢复失败: 读取文件错误")
            return

        try:
            pools_data = data.get("pools", [])
            restored: Dict[str, ResourcePool] = {}
            for pool_data in pools_data:
                pool = ResourcePool.from_dict(pool_data)
                restored[pool.name] = pool
            self.pools = restored

            cluster = data.get("cluster", {})
            self.cluster_total = ResourceSpec.from_dict(cluster.get("total", {}))
            self.cluster_available = ResourceSpec.from_dict(cluster.get("available", {}))
            self.cluster_committed = ResourceSpec.from_dict(cluster.get("committed", {}))

            logger.info("资源池状态从 %s 恢复: %d pools", state_file, len(self.pools))
        except Exception:  # pragma: no cover - defensive
            logger.exception("资源池状态恢复失败: 数据格式不正确")
            # 如果恢复失败，为避免错误状态继续使用，清空缓存
            self.pools = {}
            self.cluster_total = ResourceSpec()
            self.cluster_available = ResourceSpec()
            self.cluster_committed = ResourceSpec()

    # ------------------------------------------------------------------
    # Pool lifecycle operations

    def create_pool(
        self,
        name: str,
        labels: dict[str, str],
        resources: dict[str, float],
        target_agents: int = 0,
        placement_strategy: str = "STRICT_PACK",
    ) -> dict:
        """Register a new resource pool with quota validation."""
        if name in self.pools:
            return {"success": False, "error": f"Pool '{name}' already exists"}

        self._update_cluster_inventory()

        base_spec = ResourceSpec.from_dict(resources)
        default_agent_resources: Optional[dict[str, float]] = None

        if target_agents > 0:
            capacity_spec = base_spec.multiply(target_agents)
            default_agent_resources = base_spec.to_dict()
        else:
            capacity_spec = base_spec

        pool = ResourcePool(
            name=name,
            labels=labels,
            capacity=capacity_spec.to_dict(),
            default_agent_resources=default_agent_resources,
            target_agents=target_agents,
            placement_strategy=placement_strategy,
        )

        required = pool.ledger.quota.capacity
        cluster_required = ResourceSpec(cpu=required.cpu, memory=required.memory, gpu=required.gpu)
        remaining = self._remaining_capacity()

        if required.cpu < 0 or required.memory < 0 or required.gpu < 0:
            return {"success": False, "error": "Pool quota must be non-negative"}

        logger.info(
            "资源池[%s] 请求配额: 需求=%s 集群剩余=%s",
            name,
            required,
            remaining,
        )

        if (
            remaining.cpu < cluster_required.cpu
            or remaining.memory < cluster_required.memory
            or remaining.gpu < cluster_required.gpu
        ):
            return {
                "success": False,
                "error": "Insufficient cluster capacity for requested pool quota",
                "cluster": self._cluster_snapshot(),
            }

        self.cluster_committed = self.cluster_committed + cluster_required
        self.pools[name] = pool
        self._save_state()

        logger.info(
            "资源池[%s] 创建完成: 目标Agent=%s 总承诺(cpu=%.2f, memory=%.2f, gpu=%.2f) 剩余=%s",
            name,
            target_agents,
            self.cluster_committed.cpu,
            self.cluster_committed.memory,
            self.cluster_committed.gpu,
            self._remaining_capacity(),
        )
        return {
            "success": True,
            "pool": pool.to_dict(),
            "cluster": self._cluster_snapshot(),
        }

    def get_pool(self, name: str) -> Optional[dict]:
        """Return pool metadata if available."""
        pool = self.pools.get(name)
        return pool.to_dict() if pool else None

    def list_pools(self) -> list[dict]:
        """List all registered pools."""
        return [pool.to_dict() for pool in self.pools.values()]

    # ------------------------------------------------------------------
    # Reservation helpers (skeleton implementations)

    def reserve_agent_slot(self, pool_name: str, resources: dict[str, float]) -> dict:
        pool = self.pools.get(pool_name)
        if pool is None:
            return {"success": False, "error": f"Pool '{pool_name}' not found"}
        requested = ResourceSpec.from_dict(resources)
        logger.debug(
            "资源池[%s] 申请资源 %s (已用=%s, 已保留=%s)",
            pool_name,
            requested,
            pool.ledger.used,
            pool.ledger.reserved,
        )
        try:
            pool.ledger.reserve(requested)
        except ValueError as exc:
            logger.warning("资源池[%s] 预留失败: %s", pool_name, exc)
            return {"success": False, "error": str(exc)}
        logger.info(
            "资源池[%s] 成功预留资源 %s (已用=%s, 已保留=%s)",
            pool_name,
            requested,
            pool.ledger.used,
            pool.ledger.reserved,
        )
        self._save_state()
        return {"success": True, "pool": pool.to_dict(), "reserved": requested.to_dict()}

    def commit_agent_slot(self, pool_name: str, resources: dict[str, float]) -> dict:
        pool = self.pools.get(pool_name)
        if pool is None:
            return {"success": False, "error": f"Pool '{pool_name}' not found"}
        requested = ResourceSpec.from_dict(resources)
        try:
            pool.ledger.commit(requested)
        except ValueError as exc:
            return {"success": False, "error": str(exc)}
        logger.info(
            "资源池[%s] 资源提交 %s (已用=%s, 已保留=%s)",
            pool_name,
            requested,
            pool.ledger.used,
            pool.ledger.reserved,
        )
        self._save_state()
        return {"success": True, "pool": pool.to_dict()}

    def release_agent_slot(self, pool_name: str, resources: dict[str, float]) -> dict:
        pool = self.pools.get(pool_name)
        if pool is None:
            return {"success": False, "error": f"Pool '{pool_name}' not found"}
        release_spec = ResourceSpec.from_dict(resources)
        try:
            pool.ledger.release(release_spec)
        except ValueError as exc:
            return {"success": False, "error": str(exc)}
        logger.info(
            "资源池[%s] 释放资源 %s (已用=%s, 已保留=%s)",
            pool_name,
            release_spec,
            pool.ledger.used,
            pool.ledger.reserved,
        )
        self._save_state()
        return {"success": True, "pool": pool.to_dict()}

    def cancel_agent_reservation(self, pool_name: str, resources: dict[str, float]) -> dict:
        pool = self.pools.get(pool_name)
        if pool is None:
            return {"success": False, "error": f"Pool '{pool_name}' not found"}
        requested = ResourceSpec.from_dict(resources)
        try:
            pool.ledger.cancel(requested)
        except ValueError as exc:
            return {"success": False, "error": str(exc)}
        logger.info(
            "资源池[%s] 取消预留 %s (已用=%s, 已保留=%s)",
            pool_name,
            requested,
            pool.ledger.used,
            pool.ledger.reserved,
        )
        self._save_state()
        return {"success": True, "pool": pool.to_dict()}

    def snapshot_state(self) -> dict:
        """Expose current state for inspection/testing."""
        return {
            "cluster": self._cluster_snapshot(),
            "pools": [pool.to_dict() for pool in self.pools.values()],
        }
