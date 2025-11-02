"""
PlacementGroup Pool Manager - 动态 PG 池化管理
==================================================

核心功能：
1. 为每个 Pool 维护一个 PG 池（预留高优 + 动态普通）
2. PG 的创建、分配、释放、回收、销毁
3. 支持高优 PG 预留，保障高优任务快速启动
4. PG 复用机制，降低创建开销

设计理念：
- 每个 Agent 独占一个 PG，实现精确的资源隔离
- 高优 PG 预创建并保留，确保高优任务零等待
- 动态 PG 按需创建，闲置时可销毁或复用
- 与 PreemptionController 配合，实现快速抢占

"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import ray
from ray.util.placement_group import PlacementGroup, remove_placement_group, placement_group

from schedulemesh.core.utils import configure_runtime_logging

logger = logging.getLogger(__name__)


class PGStatus(Enum):
    """PlacementGroup 状态"""
    CREATING = "creating"      # 创建中
    READY = "ready"            # 就绪，未分配
    ALLOCATED = "allocated"    # 已分配给 Agent
    RELEASING = "releasing"    # 释放中
    FAILED = "failed"          # 创建失败
    DESTROYED = "destroyed"    # 已销毁


@dataclass
class PGSpec:
    """PlacementGroup 资源规格"""
    cpu: float = 0.0
    memory: float = 0.0  # MB
    gpu: float = 0.0
    custom_resources: Optional[Dict[str, float]] = None
    
    @staticmethod
    def from_dict(spec_dict: dict) -> "PGSpec":
        """
        从字典创建 PGSpec，自动提取自定义资源
        
        支持两种格式：
        1. {"cpu": 2.0, "memory": 1024, "gpu": 1.0, "custom_resources": {"special": 1.0}}
        2. {"cpu": 2.0, "memory": 1024, "gpu": 1.0, "special": 1.0}  # 自动识别
        """
        cpu = float(spec_dict.get("cpu", 0.0))
        memory = float(spec_dict.get("memory", 0.0))
        gpu = float(spec_dict.get("gpu", 0.0))
        
        # 提取自定义资源
        custom = {}
        if "custom_resources" in spec_dict and spec_dict["custom_resources"]:
            custom.update(spec_dict["custom_resources"])
        
        # 自动识别标准键之外的键作为自定义资源
        standard_keys = {"cpu", "memory", "gpu", "custom_resources"}
        for key, value in spec_dict.items():
            if key not in standard_keys and isinstance(value, (int, float)):
                custom[key] = float(value)
        
        return PGSpec(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            custom_resources=custom if custom else None
        )
    
    def to_bundle(self) -> dict:
        """转换为 Ray PlacementGroup bundle 格式"""
        bundle = {}
        
        if self.cpu > 0:
            bundle["CPU"] = self.cpu
        
        if self.memory > 0:
            # Ray 需要 bytes，这里输入是 MB
            bundle["memory"] = int(self.memory * 1024 * 1024)
        
        if self.gpu > 0:
            bundle["GPU"] = self.gpu
        
        if self.custom_resources:
            bundle.update(self.custom_resources)
        
        return bundle
    
    def matches(self, other: "PGSpec", *, exact: bool = False) -> bool:
        """
        检查规格是否匹配（包括自定义资源）
        
        Args:
            other: 要匹配的规格
            exact: True=精确匹配, False=大于等于即可
        """
        # 检查标准资源
        if exact:
            if not (self.cpu == other.cpu and self.memory == other.memory and self.gpu == other.gpu):
                return False
        else:
            if not (self.cpu >= other.cpu and self.memory >= other.memory and self.gpu >= other.gpu):
                return False
        
        # 检查自定义资源
        other_custom = other.custom_resources or {}
        if other_custom:
            self_custom = self.custom_resources or {}
            for key, value in other_custom.items():
                if exact:
                    if self_custom.get(key, 0.0) != value:
                        return False
                else:
                    if self_custom.get(key, 0.0) < value:
                        return False
        
        return True
    
    def __hash__(self):
        # 包含自定义资源的哈希
        custom_tuple = tuple(sorted((self.custom_resources or {}).items()))
        return hash((self.cpu, self.memory, self.gpu, custom_tuple))
    
    def __repr__(self):
        custom_str = f", custom={self.custom_resources}" if self.custom_resources else ""
        return f"PGSpec(cpu={self.cpu}, mem={self.memory}MB, gpu={self.gpu}{custom_str})"


@dataclass
class PGRecord:
    """PlacementGroup 记录"""
    pg_id: str
    spec: PGSpec
    pg_handle: Optional[PlacementGroup]
    status: PGStatus
    pool_name: str
    priority_tier: str  # "high" | "normal" | "low"
    strategy: str       # Ray PG strategy: "STRICT_PACK", "PACK", "SPREAD"
    allocated_to: Optional[str] = None  # Agent name
    created_at: float = 0.0
    allocated_at: Optional[float] = None
    released_at: Optional[float] = None
    reuse_count: int = 0  # 复用次数
    
    def is_available(self) -> bool:
        """是否可用于分配"""
        return self.status == PGStatus.READY and self.allocated_to is None
    
    def to_dict(self) -> dict:
        """序列化（不包含 pg_handle）"""
        return {
            "pg_id": self.pg_id,
            "spec": {
                "cpu": self.spec.cpu,
                "memory": self.spec.memory,
                "gpu": self.spec.gpu,
            },
            "status": self.status.value,
            "pool_name": self.pool_name,
            "priority_tier": self.priority_tier,
            "strategy": self.strategy,
            "allocated_to": self.allocated_to,
            "created_at": self.created_at,
            "allocated_at": self.allocated_at,
            "released_at": self.released_at,
            "reuse_count": self.reuse_count,
        }


@dataclass
class PoolPGConfig:
    """Pool 的 PG 池配置"""
    pool_name: str
    high_priority_pg_specs: List[PGSpec] = field(default_factory=list)
    enable_dynamic_pgs: bool = True
    max_dynamic_pgs: int = 20
    pg_strategy: str = "STRICT_PACK"
    enable_pg_reuse: bool = True  # 是否启用 PG 复用
    pg_creation_timeout: float = 60.0  # PG 创建超时（秒）


@ray.remote
class PlacementGroupPoolManager:
    """
    PlacementGroup 池化管理器
    
    核心职责：
    1. PG 生命周期管理（创建、分配、释放、销毁）
    2. 按优先级分层管理（高优预留 + 动态池）
    3. PG 复用优化
    4. 与 PreemptionController 配合快速抢占
    """
    
    def __init__(self):
        configure_runtime_logging()
        
        # PG 记录: {pg_id: PGRecord}
        self.pg_records: Dict[str, PGRecord] = {}
        
        # Pool 配置: {pool_name: PoolPGConfig}
        self.pool_configs: Dict[str, PoolPGConfig] = {}
        
        # Agent → PG 映射: {agent_name: pg_id}
        self.agent_to_pg: Dict[str, str] = {}
        
        # Pool → PG 列表映射: {pool_name: [pg_id, ...]}
        self.pool_to_pgs: Dict[str, List[str]] = {}
        
        logger.info("PlacementGroupPoolManager 初始化完成")
    
    # ================================================================
    # Pool 配置与初始化
    # ================================================================
    
    def configure_pool(
        self,
        pool_name: str,
        *,
        high_priority_pg_specs: Optional[List[dict]] = None,
        enable_dynamic_pgs: bool = True,
        max_dynamic_pgs: int = 20,
        pg_strategy: str = "STRICT_PACK",
        enable_pg_reuse: bool = True,
    ) -> dict:
        """
        配置 Pool 的 PG 池
        
        Args:
            pool_name: 资源池名称
            high_priority_pg_specs: 高优 PG 规格列表（预创建），格式: [{"cpu": 4, "memory": 8000, "gpu": 1}, ...]
            enable_dynamic_pgs: 是否允许动态创建 PG
            max_dynamic_pgs: 最大动态 PG 数量
            pg_strategy: Ray PlacementGroup 策略
            enable_pg_reuse: 是否启用 PG 复用
        
        Returns:
            {"success": bool, "pool_name": str, "high_priority_pgs": [pg_id, ...]}
        """
        # 如果 Pool 已经配置，先清理已有的 PG 以防止资源泄露
        if pool_name in self.pool_configs:
            logger.warning(
                f"Pool [{pool_name}] 已配置，正在清理已有 PG 以防止资源泄露..."
            )
            self._cleanup_pool_pgs(pool_name)
        
        # 转换规格
        hp_specs = []
        if high_priority_pg_specs:
            for spec_dict in high_priority_pg_specs:
                hp_specs.append(PGSpec.from_dict(spec_dict))
        
        config = PoolPGConfig(
            pool_name=pool_name,
            high_priority_pg_specs=hp_specs,
            enable_dynamic_pgs=enable_dynamic_pgs,
            max_dynamic_pgs=max_dynamic_pgs,
            pg_strategy=pg_strategy,
            enable_pg_reuse=enable_pg_reuse,
        )
        
        self.pool_configs[pool_name] = config
        self.pool_to_pgs[pool_name] = []
        
        # 预创建高优 PG
        created_pgs = []
        for i, spec in enumerate(hp_specs):
            pg_id = f"{pool_name}-hp-{i}-{int(time.time() * 1000)}"
            result = self._create_pg(
                pg_id=pg_id,
                spec=spec,
                pool_name=pool_name,
                priority_tier="high",
                strategy=pg_strategy,
            )
            if result.get("success"):
                created_pgs.append(pg_id)
        
        logger.info(
            f"Pool [{pool_name}] PG 池配置完成: "
            f"预创建高优 PG={len(created_pgs)}/{len(hp_specs)}, "
            f"允许动态 PG={enable_dynamic_pgs}, 最大动态数={max_dynamic_pgs}"
        )
        
        return {
            "success": True,
            "pool_name": pool_name,
            "high_priority_pgs": created_pgs,
            "config": {
                "enable_dynamic_pgs": enable_dynamic_pgs,
                "max_dynamic_pgs": max_dynamic_pgs,
                "pg_strategy": pg_strategy,
                "enable_pg_reuse": enable_pg_reuse,
            },
        }
    
    # ================================================================
    # PG 分配与释放（核心 API）
    # ================================================================
    
    def allocate_pg(
        self,
        agent_name: str,
        pool_name: str,
        spec_dict: dict,
        priority: float = 5.0,
    ) -> dict:
        """
        为 Agent 分配 PlacementGroup
        
        分配策略：
        1. priority >= 8.0: 优先使用预留的高优 PG
        2. 尝试复用已有的空闲 PG（规格匹配）
        3. 动态创建新 PG
        
        Args:
            agent_name: Agent 名称
            pool_name: 资源池名称
            spec_dict: 资源规格字典 {"cpu": float, "memory": float, "gpu": float}
            priority: 优先级
        
        Returns:
            {
                "success": bool,
                "pg_id": str,
                "pg_handle": PlacementGroup,
                "spec": dict,
                "priority_tier": str,
                "error": str (if failed)
            }
        """
        # 检查是否已分配
        if agent_name in self.agent_to_pg:
            existing_pg_id = self.agent_to_pg[agent_name]
            return {
                "success": False,
                "error": f"Agent {agent_name} already allocated to PG {existing_pg_id}"
            }
        
        # 检查 Pool 是否配置
        if pool_name not in self.pool_configs:
            return {
                "success": False,
                "error": f"Pool {pool_name} not configured for PG management"
            }
        
        spec = PGSpec.from_dict(spec_dict)
        
        # 策略 1: 高优任务优先使用预留 PG
        if priority >= 8.0:
            pg_record = self._find_available_pg(
                pool_name=pool_name,
                priority_tier="high",
                spec=spec,
            )
            if pg_record:
                logger.info(f"Agent {agent_name} 使用预留高优 PG {pg_record.pg_id}")
                return self._allocate_pg_to_agent(agent_name, pg_record)
        
        # 策略 2: 尝试复用已有空闲 PG
        config = self.pool_configs[pool_name]
        if config.enable_pg_reuse:
            pg_record = self._find_available_pg(
                pool_name=pool_name,
                priority_tier="normal",
                spec=spec,
            )
            if pg_record:
                logger.info(
                    f"Agent {agent_name} 复用空闲 PG {pg_record.pg_id} "
                    f"(第 {pg_record.reuse_count + 1} 次复用)"
                )
                return self._allocate_pg_to_agent(agent_name, pg_record)
        
        # 策略 3: 动态创建新 PG
        if not config.enable_dynamic_pgs:
            return {
                "success": False,
                "error": f"Pool {pool_name} does not allow dynamic PG creation"
            }
        
        # 检查动态 PG 数量限制
        dynamic_pg_count = len([
            pg_id for pg_id in self.pool_to_pgs.get(pool_name, [])
            if self.pg_records[pg_id].priority_tier == "normal"
        ])
        
        if dynamic_pg_count >= config.max_dynamic_pgs:
            return {
                "success": False,
                "error": f"Pool {pool_name} reached max dynamic PG limit ({config.max_dynamic_pgs})"
            }
        
        # 创建新 PG
        pg_id = f"{pool_name}-dyn-{int(time.time() * 1000)}"
        logger.info(f"为 Agent {agent_name} 创建新的动态 PG {pg_id}")
        
        result = self._create_pg(
            pg_id=pg_id,
            spec=spec,
            pool_name=pool_name,
            priority_tier="normal",
            strategy=config.pg_strategy,
        )
        
        if not result.get("success"):
            return result
        
        # 分配给 Agent
        pg_record = self.pg_records[pg_id]
        return self._allocate_pg_to_agent(agent_name, pg_record)
    
    def release_pg(
        self,
        agent_name: str,
        *,
        destroy: bool = False,
        force: bool = False,
    ) -> dict:
        """
        释放 Agent 占用的 PG
        
        Args:
            agent_name: Agent 名称
            destroy: 是否销毁 PG（False 则回收到池中复用）
            force: 强制释放（即使 PG 不存在也返回成功）
        
        Returns:
            {
                "success": bool,
                "pg_id": str,
                "reused": bool,  # 是否保留用于复用
                "error": str (if failed)
            }
        """
        pg_id = self.agent_to_pg.get(agent_name)
        if not pg_id:
            if force:
                return {"success": True, "message": f"Agent {agent_name} has no PG (forced)"}
            return {"success": False, "error": f"Agent {agent_name} has no allocated PG"}
        
        pg_record = self.pg_records.get(pg_id)
        if not pg_record:
            if force:
                self.agent_to_pg.pop(agent_name, None)
                return {"success": True, "message": f"PG {pg_id} not found (forced)"}
            return {"success": False, "error": f"PG {pg_id} not found"}
        
        # 取消分配
        pg_record.allocated_to = None
        pg_record.released_at = time.time()
        pg_record.status = PGStatus.READY
        self.agent_to_pg.pop(agent_name, None)
        
        logger.info(f"PG {pg_id} 从 Agent {agent_name} 释放")
        
        # 根据调用方指示决定是否销毁
        should_destroy = destroy
        
        if should_destroy:
            return self._destroy_pg(pg_id)
        
        # 保留在池中以便复用
        return {"success": True, "pg_id": pg_id, "reused": True}
    
    def get_pg_for_agent(self, agent_name: str) -> Optional[dict]:
        """获取 Agent 分配的 PG 信息（包含 handle）"""
        pg_id = self.agent_to_pg.get(agent_name)
        if not pg_id:
            return None
        
        pg_record = self.pg_records.get(pg_id)
        if not pg_record:
            return None
        
        return {
            "pg_id": pg_id,
            "spec": {
                "cpu": pg_record.spec.cpu,
                "memory": pg_record.spec.memory,
                "gpu": pg_record.spec.gpu,
            },
            "status": pg_record.status.value,
            "priority_tier": pg_record.priority_tier,
            "pg_handle": pg_record.pg_handle,
            "reuse_count": pg_record.reuse_count,
        }
    
    # ================================================================
    # 统计与监控
    # ================================================================
    
    def get_pool_stats(self, pool_name: Optional[str] = None) -> dict:
        """
        获取 Pool 的 PG 池统计信息
        
        Args:
            pool_name: Pool 名称，None 则返回所有 Pool 的统计
        
        Returns:
            {
                "pool_name": str,
                "total_pgs": int,
                "high_priority_pgs": int,
                "dynamic_pgs": int,
                "allocated_pgs": int,
                "available_pgs": int,
                "by_status": {"ready": int, "allocated": int, ...}
            }
        """
        if pool_name:
            pools = [pool_name]
        else:
            pools = list(self.pool_configs.keys())
        
        stats = {}
        for pname in pools:
            pool_pgs = [
                self.pg_records[pg_id]
                for pg_id in self.pool_to_pgs.get(pname, [])
                if pg_id in self.pg_records
            ]
            
            pool_stats = {
                "pool_name": pname,
                "total_pgs": len(pool_pgs),
                "high_priority_pgs": sum(1 for r in pool_pgs if r.priority_tier == "high"),
                "dynamic_pgs": sum(1 for r in pool_pgs if r.priority_tier == "normal"),
                "allocated_pgs": sum(1 for r in pool_pgs if r.allocated_to is not None),
                "available_pgs": sum(1 for r in pool_pgs if r.is_available()),
                "total_reuse_count": sum(r.reuse_count for r in pool_pgs),
                "by_status": {},
            }
            
            for status in PGStatus:
                pool_stats["by_status"][status.value] = sum(
                    1 for r in pool_pgs if r.status == status
                )
            
            stats[pname] = pool_stats
        
        if pool_name:
            return stats.get(pool_name, {})
        
        return {"pools": stats}
    
    def list_pgs(self, pool_name: Optional[str] = None) -> dict:
        """列出 PG 详情（不包含 handle）"""
        if pool_name:
            pg_ids = self.pool_to_pgs.get(pool_name, [])
        else:
            pg_ids = list(self.pg_records.keys())
        
        pgs = []
        for pg_id in pg_ids:
            record = self.pg_records.get(pg_id)
            if record:
                pgs.append(record.to_dict())
        
        return {"success": True, "pgs": pgs}
    
    # ================================================================
    # 内部方法
    # ================================================================
    
    def _create_pg(
        self,
        pg_id: str,
        spec: PGSpec,
        pool_name: str,
        priority_tier: str,
        strategy: str,
    ) -> dict:
        """创建 PlacementGroup"""
        try:
            bundle = spec.to_bundle()
            
            if not bundle:
                return {
                    "success": False,
                    "error": f"PG {pg_id} has empty bundle (no resources specified)"
                }
            
            logger.debug(
                f"创建 PG {pg_id}: pool={pool_name}, tier={priority_tier}, "
                f"bundle={bundle}, strategy={strategy}"
            )
            
            config = self.pool_configs[pool_name]
            pg = None  # 初始化为 None，用于异常处理中判断是否需要清理
            pg = placement_group(
                bundles=[bundle],
                strategy=strategy,
                name=pg_id,
            )
            
            # 等待 PG 就绪
            timeout = config.pg_creation_timeout
            ray.get(pg.ready(), timeout=timeout)
            
            # 记录
            record = PGRecord(
                pg_id=pg_id,
                spec=spec,
                pg_handle=pg,
                status=PGStatus.READY,
                pool_name=pool_name,
                priority_tier=priority_tier,
                strategy=strategy,
                created_at=time.time(),
            )
            self.pg_records[pg_id] = record
            
            # 添加到 Pool 的 PG 列表
            if pool_name not in self.pool_to_pgs:
                self.pool_to_pgs[pool_name] = []
            self.pool_to_pgs[pool_name].append(pg_id)
            
            logger.info(f"PG {pg_id} 创建成功: {bundle}")
            
            return {"success": True, "pg_id": pg_id}
            
        except Exception as exc:
            logger.exception(f"创建 PG {pg_id} 失败")
            
            # 如果 PG 已创建但等待失败，需要清理以防止资源泄露
            if 'pg' in locals() and pg is not None:
                try:
                    logger.warning(f"清理失败的 PG {pg_id} 以防止资源泄露")
                    remove_placement_group(pg)
                except Exception as cleanup_exc:
                    logger.warning(f"清理失败的 PG {pg_id} 时发生错误: {cleanup_exc}")
            
            # 记录失败状态
            record = PGRecord(
                pg_id=pg_id,
                spec=spec,
                pg_handle=None,
                status=PGStatus.FAILED,
                pool_name=pool_name,
                priority_tier=priority_tier,
                strategy=strategy,
                created_at=time.time(),
            )
            self.pg_records[pg_id] = record
            
            return {"success": False, "error": str(exc), "pg_id": pg_id}
    
    def _destroy_pg(self, pg_id: str) -> dict:
        """销毁 PlacementGroup"""
        pg_record = self.pg_records.get(pg_id)
        if not pg_record:
            return {"success": False, "error": f"PG {pg_id} not found"}
        
        if pg_record.pg_handle:
            try:
                remove_placement_group(pg_record.pg_handle)
                logger.info(f"PG {pg_id} 已销毁")
            except Exception as exc:
                logger.warning(f"销毁 PG {pg_id} 失败: {exc}")
        
        # 从 pool_to_pgs 中移除
        pool_name = pg_record.pool_name
        if pool_name in self.pool_to_pgs:
            try:
                self.pool_to_pgs[pool_name].remove(pg_id)
            except ValueError:
                pass
        
        # 移除记录
        pg_record.status = PGStatus.DESTROYED
        self.pg_records.pop(pg_id, None)
        
        return {"success": True, "pg_id": pg_id, "destroyed": True}
    
    def _cleanup_pool_pgs(self, pool_name: str) -> None:
        """
        清理 Pool 的所有 PG，防止重新配置时的资源泄露
        
        这个方法在重新配置 Pool 时调用，确保：
        1. 所有已有的 PG 都被正确销毁或释放
        2. 清理 pg_records、agent_to_pg、pool_to_pgs 中的记录
        3. 防止 PG "丢失"导致的资源泄露
        """
        if pool_name not in self.pool_to_pgs:
            return
        
        pg_ids = list(self.pool_to_pgs[pool_name])
        logger.info(f"清理 Pool [{pool_name}] 的 {len(pg_ids)} 个 PG...")
        
        for pg_id in pg_ids:
            pg_record = self.pg_records.get(pg_id)
            if not pg_record:
                continue
            
            # 如果 PG 已分配给 Agent，先取消分配
            if pg_record.allocated_to:
                agent_name = pg_record.allocated_to
                logger.warning(
                    f"PG {pg_id} 仍分配给 Agent {agent_name}，强制释放"
                )
                self.agent_to_pg.pop(agent_name, None)
                pg_record.allocated_to = None
            
            # 销毁 PG
            self._destroy_pg(pg_id)
        
        # 清空 pool_to_pgs 列表
        self.pool_to_pgs[pool_name] = []
        logger.info(f"Pool [{pool_name}] 的 PG 清理完成")
    
    def _find_available_pg(
        self,
        pool_name: str,
        priority_tier: str,
        spec: PGSpec,
    ) -> Optional[PGRecord]:
        """查找可用的 PG（规格匹配且未分配）"""
        pg_ids = self.pool_to_pgs.get(pool_name, [])
        
        for pg_id in pg_ids:
            record = self.pg_records.get(pg_id)
            if not record:
                continue
            
            if (
                record.priority_tier == priority_tier
                and record.is_available()
                and record.spec.matches(spec, exact=False)
            ):
                return record
        
        return None
    
    def _allocate_pg_to_agent(self, agent_name: str, pg_record: PGRecord) -> dict:
        """将 PG 分配给 Agent"""
        pg_record.allocated_to = agent_name
        pg_record.allocated_at = time.time()
        pg_record.status = PGStatus.ALLOCATED
        pg_record.reuse_count += 1
        self.agent_to_pg[agent_name] = pg_record.pg_id
        
        logger.info(
            f"PG {pg_record.pg_id} 分配给 Agent {agent_name} "
            f"(tier={pg_record.priority_tier}, reuse_count={pg_record.reuse_count})"
        )
        
        return {
            "success": True,
            "pg_id": pg_record.pg_id,
            "pg_handle": pg_record.pg_handle,
            "spec": {
                "cpu": pg_record.spec.cpu,
                "memory": pg_record.spec.memory,
                "gpu": pg_record.spec.gpu,
            },
            "priority_tier": pg_record.priority_tier,
            "reuse_count": pg_record.reuse_count,
        }


