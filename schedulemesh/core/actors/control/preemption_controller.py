"""
PreemptionController actor - 抢占控制器实现。

支持：
- 同 pool 内抢占（优先级高，不受限制）
- 跨 pool 抢占（受隔离策略保护）
- 抢占判定算法：preempt_score = Δpriority + κ × remaining_time
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import ray
from ray.util import metrics

from schedulemesh.config.policy import (
    PREEMPTION_AGGRESSIVENESS_LEVELS,
    PreemptionAggressiveness,
    resolve_preemption_aggressiveness,
)
from schedulemesh.core.entities.resource_pool import ResourceSpec

logger = logging.getLogger(__name__)


@dataclass
class PreemptionPolicy:
    """抢占策略配置"""
    
    # 同 pool 内抢占阈值（优先级差值）
    same_pool_priority_threshold: float = 1.0
    
    # 跨 pool 抢占阈值（优先级差值，通常设置更高）
    cross_pool_priority_threshold: float = 5.0
    
    # 剩余时间权重系数 κ
    remaining_time_weight: float = 0.1
    
    # 最小抢占得分阈值
    min_preempt_score: float = 0.5
    
    # 是否允许跨 pool 抢占
    enable_cross_pool_preemption: bool = True
    
    # 被保护的 pool 标签（不允许被跨 pool 抢占）
    protected_pools: List[str] = field(default_factory=list)
    
    # ========== Label 级别抢占配置（新增） ==========
    # 基于 label 的抢占规则（定义 label 级别的优先级层次）
    # 格式: {"label_key": {"high_value": ["low_value1", "low_value2"]}}
    # 例如: {"tier": {"premium": ["standard", "batch"], "standard": ["batch"]}}
    # 说明：
    #   - label_key: 任意的 label 键名（如 "tier", "user_tier", "priority_class" 等）
    #   - high_value: 高优先级的 label 值，可以抢占列表中的低优先级值
    #   - 支持多个 label_key，只要任一规则匹配即可触发抢占
    #   - 例如配置 {"tier": {"premium": ["standard"]}} 表示 tier=premium 的任务可以抢占 tier=standard 的任务
    label_preemption_rules: Dict[str, Dict[str, List[str]]] = field(default_factory=dict)
    
    # label 抢占的优先级阈值（针对基于 label 的抢占）
    # 通常设置为较低的值（如 0.5），使得满足 label 规则的任务更容易触发抢占
    # 比 same_pool_priority_threshold 更低，允许更灵活的抢占策略
    label_priority_threshold: float = 0.5
    
    # 是否启用 label 级别抢占（优先于 pool 级别检查）
    # 设置为 True 时，会优先检查 label 规则，再回退到 pool 规则
    # 设置为 False 时，只使用传统的 pool 级别抢占
    enable_label_preemption: bool = True


@dataclass
class TaskState:
    """任务状态信息"""
    
    task_id: str
    agent_name: str
    pool_name: str
    priority: float
    labels: Dict[str, str]
    start_time: float
    estimated_duration: float = 0.0  # 预估执行时长（秒）
    payload: Any = None
    last_preemption_reason: Optional[str] = None
    resources: Optional[ResourceSpec] = None  # 任务占用的资源
    
    def remaining_time(self, current_time: Optional[float] = None) -> float:
        """计算剩余执行时间"""
        if current_time is None:
            current_time = time.time()
        elapsed = current_time - self.start_time
        if self.estimated_duration > 0:
            return max(0.0, self.estimated_duration - elapsed)
        # 如果没有预估时长，使用已执行时间作为参考
        return elapsed * 0.5  # 假设还需要当前执行时间的一半


@dataclass
class PreemptionCandidate:
    """抢占候选项"""
    
    task_state: TaskState
    preempt_score: float
    reason: str


StatePreserver = Callable[[TaskState], Dict[str, Any]]
StateRestorer = Callable[[Dict[str, Any]], None]


@ray.remote
class PreemptionControllerActor:
    """抢占控制器 - 管理任务优先级和抢占决策"""

    def __init__(self, policy: Optional[PreemptionPolicy] = None):
        self.policy = policy or PreemptionPolicy()
        
        # 运行中的任务状态 {task_id: TaskState}
        self.running_tasks: Dict[str, TaskState] = {}
        
        # 抢占历史记录
        self.preemption_history: List[Dict[str, Any]] = []
        
        # 自定义状态处理器 {pool_name: (preserver, restorer)}
        self.state_handlers: Dict[str, Tuple[StatePreserver, StateRestorer]] = {}
        
        # 初始化 Ray metrics（在 actor 初始化时创建，避免重复创建）
        self.preemption_eval_latency_gauge = metrics.Gauge(
            name="schedulemesh_preemption_evaluation_latency_ms",
            description="Preemption evaluation latency (ms)",
            tag_keys=("pool",)
        )
        self.preemption_exec_latency_gauge = metrics.Gauge(
            name="schedulemesh_preemption_execution_latency_ms",
            description="Preemption execution latency (ms)",
            tag_keys=("pool", "reason")
        )
        self.preemption_count_counter = metrics.Counter(
            name="schedulemesh_preemption_count",
            description="Preemption count",
            tag_keys=("pool", "reason")
        )
        
        logger.info(
            "PreemptionControllerActor 初始化 same_pool_threshold=%.2f cross_pool_threshold=%.2f",
            self.policy.same_pool_priority_threshold,
            self.policy.cross_pool_priority_threshold,
        )

    def register_state_handlers(
        self,
        pool_name: str,
        preserver: StatePreserver,
        restorer: StateRestorer,
    ) -> dict:
        """注册自定义状态保存/恢复处理器"""
        self.state_handlers[pool_name] = (preserver, restorer)
        logger.info("已注册自定义状态处理器 pool=%s", pool_name)
        return {"success": True}

    def register_running_task(
        self,
        task_id: str,
        agent_name: str,
        pool_name: str,
        priority: float,
        labels: Optional[Dict[str, str]] = None,
        estimated_duration: float = 0.0,
        payload: Any = None,
        resources: Optional[ResourceSpec] = None,
    ) -> dict:
        """注册一个正在运行的任务"""
        task_state = TaskState(
            task_id=task_id,
            agent_name=agent_name,
            pool_name=pool_name,
            priority=priority,
            labels=labels or {},
            start_time=time.time(),
            estimated_duration=estimated_duration,
            payload=payload,
            resources=resources,
        )
        self.running_tasks[task_id] = task_state
        logger.debug(
            "注册运行任务 task_id=%s agent=%s pool=%s priority=%.2f resources=%s",
            task_id,
            agent_name,
            pool_name,
            priority,
            resources,
        )
        return {"success": True, "task_state": self._task_state_to_dict(task_state)}

    def unregister_task(self, task_id: str) -> dict:
        """任务完成，取消注册"""
        task_state = self.running_tasks.pop(task_id, None)
        if task_state is None:
            return {"success": False, "error": f"Task '{task_id}' not found"}
        logger.debug("任务完成 task_id=%s", task_id)
        return {"success": True}

    def evaluate_preemption(
        self,
        incoming_task_priority: float,
        incoming_task_pool: str,
        incoming_task_resources: Optional[ResourceSpec] = None,
        incoming_task_labels: Optional[Dict[str, str]] = None,
        agent_manager_handle: Optional[ray.actor.ActorHandle] = None,
        resource_pool_manager_handle: Optional[ray.actor.ActorHandle] = None,
    ) -> dict:
        """
        评估是否需要抢占，返回抢占候选列表
        
        Args:
            incoming_task_priority: 新任务的优先级
            incoming_task_pool: 新任务的资源池
            incoming_task_resources: 新任务需要的资源（可选，如果不提供会从pool获取默认资源）
            incoming_task_labels: 新任务的标签
            agent_manager_handle: AgentManager的句柄，用于获取实际资源占用
            resource_pool_manager_handle: ResourcePoolManager的句柄，用于获取pool默认资源
            
        Returns:
            {
                "should_preempt": bool,
                "candidates": List[PreemptionCandidate],
                "reason": str
            }
        """
        # 记录评估开始时间
        start_time = time.time()
        if not self.running_tasks:
            # 记录评估耗时（即使没有任务）
            evaluation_latency_ms = (time.time() - start_time) * 1000
            
            # 使用 Ray metrics 记录性能指标
            self.preemption_eval_latency_gauge.set(
                evaluation_latency_ms, 
                tags={"pool": incoming_task_pool}
            )
            
            return {
                "should_preempt": False,
                "candidates": [],
                "reason": "No running tasks to preempt",
                "evaluation_latency_ms": evaluation_latency_ms,
            }

        current_time = time.time()
        candidates: List[PreemptionCandidate] = []

        incoming_labels_dict = incoming_task_labels or {}
        
        # ========== 自动获取新任务资源需求 ==========
        # 如果没有提供资源信息，尝试从pool获取默认资源
        if incoming_task_resources is None and resource_pool_manager_handle is not None:
            try:
                pool_info = ray.get(resource_pool_manager_handle.get_pool.remote(incoming_task_pool))
                if pool_info and "default_agent_resources" in pool_info and pool_info["default_agent_resources"]:
                    incoming_task_resources = ResourceSpec.from_dict(pool_info["default_agent_resources"])
                    logger.debug(
                        "从Pool获取默认资源 incoming_pool=%s default_resources=%s",
                        incoming_task_pool,
                        incoming_task_resources,
                    )
            except Exception as exc:
                logger.warning(
                    "获取Pool默认资源失败 pool=%s error=%s",
                    incoming_task_pool,
                    exc,
                )

        # 如果提供了资源信息，进行资源验证
        if incoming_task_resources is not None:
            logger.debug(
                "抢占评估包含资源验证 incoming_resources=%s",
                incoming_task_resources
            )

        for task_id, task_state in self.running_tasks.items():
            # ========== 0. 资源验证（新增） ==========
            # 如果提供了新任务的资源需求，验证被抢占任务的资源是否足够
            if incoming_task_resources is not None:
                # 优先使用TaskState中的资源信息
                victim_resources = task_state.resources
                
                # 如果没有资源信息，尝试从AgentManager获取实际资源占用
                if victim_resources is None and agent_manager_handle is not None:
                    try:
                        agent_info = ray.get(agent_manager_handle.get_agent.remote(task_state.agent_name))
                        if agent_info and "resources" in agent_info:
                            victim_resources = ResourceSpec.from_dict(agent_info["resources"])
                            logger.debug(
                                "从AgentManager获取资源信息 task_id=%s agent=%s resources=%s",
                                task_id,
                                task_state.agent_name,
                                victim_resources,
                            )
                    except Exception as exc:
                        logger.warning(
                            "获取Agent资源信息失败 task_id=%s agent=%s error=%s",
                            task_id,
                            task_state.agent_name,
                            exc,
                        )
                
                # 进行资源验证
                if victim_resources is not None:
                    if not victim_resources.has_enough(incoming_task_resources):
                        logger.debug(
                            "跳过资源不足的任务 task_id=%s victim_resources=%s required=%s",
                            task_id,
                            victim_resources,
                            incoming_task_resources,
                        )
                        continue
                    else:
                        logger.debug(
                            "任务资源充足 task_id=%s victim_resources=%s required=%s",
                            task_id,
                            victim_resources,
                            incoming_task_resources,
                        )
                else:
                    logger.warning(
                        "无法获取任务资源信息，跳过资源验证 task_id=%s agent=%s",
                        task_id,
                        task_state.agent_name,
                    )
            # 计算优先级差值（正数表示新任务优先级更高）
            priority_delta = incoming_task_priority - task_state.priority

            # 检查是否满足 label 抢占规则
            can_preempt_by_label = self._can_preempt_by_label(
                incoming_labels_dict,
                task_state.labels,
            )

            if priority_delta <= 0 and not can_preempt_by_label:
                # 如果新任务优先级不高于运行任务，且不满足 label 抢占规则，则跳过
                # 注意：label 抢占可以允许优先级相同或略低的任务抢占
                continue

            # 判断是否为跨 pool 抢占
            is_cross_pool = incoming_task_pool != task_state.pool_name

            # ========== 1. 首先检查基于 label 的抢占规则（优先级最高） ==========
            # Label 抢占提供更细粒度的控制，不受 pool 边界限制
            # 只要 label 规则匹配，即使跨 pool 也可以抢占（使用较低的阈值）
            if can_preempt_by_label:
                # Label 抢占允许一定的优先级回退
                # 例如：admin 用户可以抢占 regular 用户，即使优先级略低
                # 但优先级劣势不能过大（超过 label_priority_threshold 时拒绝）
                if priority_delta < -self.policy.label_priority_threshold:
                    logger.debug(
                        "Label 抢占优先级劣势过大 task_id=%s delta=%.2f threshold=%.2f",
                        task_id,
                        priority_delta,
                        self.policy.label_priority_threshold,
                    )
                    continue

                # 计算抢占得分
                remaining = task_state.remaining_time(current_time)
                
                # 对 label 抢占进行得分放宽：
                # 如果优先级差值小于 label_priority_threshold，提升到阈值水平
                # 这样即使优先级相同或略低，也能触发抢占
                base_score = max(priority_delta, 0.0)
                if base_score < self.policy.label_priority_threshold:
                    base_score = self.policy.label_priority_threshold
                
                # 最终得分 = 基础分 + 剩余时间权重
                preempt_score = base_score + self.policy.remaining_time_weight * remaining

                # 使用较宽松的最小得分要求（取两者中的较小值）
                min_label_score = min(
                    self.policy.min_preempt_score,
                    self.policy.label_priority_threshold,
                )
                if preempt_score < min_label_score:
                    continue

                # 标记为 label 级别抢占，加入候选列表
                reason = "label_based_preemption"
                task_state.last_preemption_reason = reason
                candidates.append(
                    PreemptionCandidate(
                        task_state=task_state,
                        preempt_score=preempt_score,
                        reason=reason,
                    )
                )
                # 跳过后续的 pool 级别检查（label 抢占已匹配）
                continue
            
            # ========== 2. 回退到传统的 pool 级别抢占检查 ==========
            # 如果 label 规则不匹配或未启用，使用传统的 pool 级别抢占逻辑
            if is_cross_pool:
                # 跨 pool 抢占检查
                if not self.policy.enable_cross_pool_preemption:
                    continue
                    
                if task_state.pool_name in self.policy.protected_pools:
                    logger.debug(
                        "任务所在pool受保护 task_id=%s pool=%s",
                        task_id,
                        task_state.pool_name,
                    )
                    continue
                
                if priority_delta < self.policy.cross_pool_priority_threshold:
                    continue
            else:
                # 同 pool 内抢占检查
                if priority_delta < self.policy.same_pool_priority_threshold:
                    continue

            # 计算抢占得分：Δpriority + κ × remaining_time
            remaining = task_state.remaining_time(current_time)
            preempt_score = priority_delta + self.policy.remaining_time_weight * remaining
            
            if preempt_score < self.policy.min_preempt_score:
                continue

            reason = "cross_pool_preemption" if is_cross_pool else "same_pool_preemption"
            task_state.last_preemption_reason = reason
            candidates.append(
                PreemptionCandidate(
                    task_state=task_state,
                    preempt_score=preempt_score,
                    reason=reason,
                )
            )

        # 优化抢占候选者选择：优先选择资源充足的候选者，然后按抢占得分排序
        def candidate_sort_key(candidate):
            # 如果提供了资源信息，优先选择资源充足的候选者
            # 返回元组：(是否资源充足, 抢占得分)
            # 使用 reverse=True 排序时，True > False，所以资源充足的会排在前面
            if incoming_task_resources is not None and candidate.task_state.resources is not None:
                has_enough = candidate.task_state.resources.has_enough(incoming_task_resources)
                return (has_enough, candidate.preempt_score)
            else:
                # 如果没有资源信息，假设资源充足，只按抢占得分排序
                return (True, candidate.preempt_score)
        
        candidates.sort(key=candidate_sort_key, reverse=True)

        # 记录评估耗时
        evaluation_latency_ms = (time.time() - start_time) * 1000
        
        # 使用 Ray metrics 记录性能指标
        self.preemption_eval_latency_gauge.set(
            evaluation_latency_ms,
            tags={"pool": incoming_task_pool}
        )
        
        logger.info(
            "抢占评估完成 incoming_priority=%.2f pool=%s candidates=%d latency=%.2fms",
            incoming_task_priority,
            incoming_task_pool,
            len(candidates),
            evaluation_latency_ms,
        )

        return {
            "should_preempt": len(candidates) > 0,
            "candidates": [self._candidate_to_dict(c) for c in candidates],
            "reason": f"Found {len(candidates)} preemption candidates",
            "evaluation_latency_ms": evaluation_latency_ms,
        }

    def execute_preemption(
        self,
        task_id: str,
        agent_handle: Optional[ray.actor.ActorHandle] = None,
        preemptor_task_id: Optional[str] = None,
    ) -> dict:
        """
        执行抢占操作
        
        Args:
            task_id: 要抢占的任务ID
            agent_handle: Agent的Ray句柄（用于取消任务）
            
        Returns:
            {
                "success": bool,
                "saved_state": dict,  # 保存的状态
                "error": str
            }
        """
        # 记录执行开始时间
        start_time = time.time()
        task_state = self.running_tasks.get(task_id)
        if task_state is None:
            return {"success": False, "error": f"Task '{task_id}' not found"}

        logger.info(
            "执行抢占 task_id=%s agent=%s pool=%s priority=%.2f",
            task_id,
            task_state.agent_name,
            task_state.pool_name,
            task_state.priority,
        )

        # 保存任务状态
        saved_state = self._save_task_state(task_state)

        # 取消任务执行（如果提供了agent_handle）
        cancel_success = True
        cancel_timeout = getattr(self.policy, "cancel_timeout", None)
        if agent_handle is not None:
            try:
                # 调用 Agent 的 cancel 方法，支持可选超时
                cancel_task = agent_handle.cancel.remote(task_id)
                if cancel_timeout:
                    ray.get(cancel_task, timeout=cancel_timeout)
                else:
                    ray.get(cancel_task)
                logger.debug("任务已取消 task_id=%s", task_id)
            except Exception as exc:
                logger.warning("取消任务失败 task_id=%s error=%s", task_id, exc)
                cancel_success = False

        # 记录抢占历史
        preemption_record = {
            "timestamp": time.time(),
            "task_id": task_id,
            "preemptor_task_id": preemptor_task_id or "unknown",
            "agent_name": task_state.agent_name,
            "pool_name": task_state.pool_name,
            "priority": task_state.priority,
            "saved_state": saved_state,
            "cancel_success": cancel_success,
            "reason": task_state.last_preemption_reason or "unknown",
        }
        self.preemption_history.append(preemption_record)

        # 从运行列表中移除
        self.running_tasks.pop(task_id, None)
        
        # 记录执行耗时
        execution_latency_ms = (time.time() - start_time) * 1000
        
        # 使用 Ray metrics 记录性能指标
        self.preemption_exec_latency_gauge.set(
            execution_latency_ms, 
            tags={
                "pool": task_state.pool_name,
                "reason": task_state.last_preemption_reason or "unknown"
            }
        )
        
        # 记录抢占次数
        self.preemption_count_counter.inc(
            tags={
                "pool": task_state.pool_name,
                "reason": task_state.last_preemption_reason or "unknown"
            }
        )

        logger.info(
            "抢占执行完成 task_id=%s latency=%.2fms success=%s",
            task_id,
            execution_latency_ms,
            cancel_success,
        )

        return {
            "success": True,
            "saved_state": saved_state,
            "cancel_success": cancel_success,
            "execution_latency_ms": execution_latency_ms,
        }

    def restore_task(self, saved_state: Dict[str, Any]) -> dict:
        """恢复被抢占的任务"""
        try:
            pool_name = saved_state.get("pool_name")
            if pool_name and pool_name in self.state_handlers:
                # 使用自定义恢复器
                _, restorer = self.state_handlers[pool_name]
                restorer(saved_state)
                logger.info("使用自定义处理器恢复任务 pool=%s", pool_name)
            else:
                # 使用默认恢复逻辑
                logger.info("使用默认处理器恢复任务 task_id=%s", saved_state.get("task_id"))
            
            return {"success": True}
        except Exception as exc:
            logger.exception("恢复任务失败 error=%s", exc)
            return {"success": False, "error": str(exc)}

    def get_preemption_stats(self) -> dict:
        """获取抢占统计信息"""
        total_preemptions = len(self.preemption_history)
        
        same_pool_count = sum(
            1 for record in self.preemption_history
            if record.get("reason") == "same_pool_preemption"
        )
        cross_pool_count = total_preemptions - same_pool_count
        
        return {
            "total_preemptions": total_preemptions,
            "same_pool_preemptions": same_pool_count,
            "cross_pool_preemptions": cross_pool_count,
            "running_tasks": len(self.running_tasks),
            "recent_preemptions": self.preemption_history[-10:],  # 最近10条
        }

    def update_policy(self, **kwargs) -> dict:
        """动态更新抢占策略"""
        # 处理语义级别的抢占积极性
        aggressiveness = kwargs.pop("preemption_aggressiveness", None)
        if aggressiveness is not None:
            level, hint = resolve_preemption_aggressiveness(aggressiveness)
            if level is None:
                logger.warning(
                    "无法解析抢占积极性配置 '%s'%s，请使用 PreemptionAggressiveness 枚举值",
                    aggressiveness,
                    f" ({hint})" if hint else "",
                )
            else:
                thresholds = PREEMPTION_AGGRESSIVENESS_LEVELS[level]
                self.policy.label_priority_threshold = thresholds[
                    "label_priority_threshold"
                ]
                self.policy.same_pool_priority_threshold = thresholds[
                    "same_pool_priority_threshold"
                ]
                logger.info(
                    "应用抢占积极性 '%s': label_threshold=%.2f, same_pool_threshold=%.2f%s",
                    level.value,
                    self.policy.label_priority_threshold,
                    self.policy.same_pool_priority_threshold,
                    f" 来源={hint}" if hint else "",
                )

        for key, value in kwargs.items():
            if hasattr(self.policy, key):
                setattr(self.policy, key, value)
                logger.info("更新抢占策略 %s=%s", key, value)
        return {"success": True, "policy": self._policy_to_dict()}

    # 私有辅助方法 --------------------------------------------------------

    def _can_preempt_by_label(
        self,
        incoming_labels: Dict[str, str],
        running_labels: Dict[str, str],
    ) -> bool:
        """
        根据 label 规则判断是否可以抢占
        
        这个方法实现了基于 label 的细粒度抢占判断逻辑。
        只要新任务和运行任务在任意一个 label 上满足配置的抢占规则，就返回 True。
        
        工作原理：
        1. 遍历所有配置的 label_preemption_rules
        2. 对每个 label_key，检查新任务和运行任务是否都有该 label
        3. 如果新任务的 label 值在规则中，且运行任务的 label 值在允许抢占的列表中，则匹配成功
        
        示例：
            配置：{"tier": {"premium": ["standard", "batch"]}}
            新任务：{"tier": "premium"}
            运行任务：{"tier": "standard"}
            结果：返回 True（premium 可以抢占 standard）
        
        Args:
            incoming_labels: 新任务的标签字典
            running_labels: 运行任务的标签字典
            
        Returns:
            True 如果根据 label 规则可以抢占，否则 False
        """
        # 检查是否启用 label 抢占
        if not self.policy.enable_label_preemption:
            return False
        
        # 检查是否配置了抢占规则
        if not self.policy.label_preemption_rules:
            return False
        
        # 遍历所有配置的 label 抢占规则
        for label_key, rules in self.policy.label_preemption_rules.items():
            # 获取新任务和运行任务在该 label 上的值
            incoming_value = incoming_labels.get(label_key)
            running_value = running_labels.get(label_key)
            
            # 如果任一任务没有这个 label，跳过该规则
            if incoming_value is None or running_value is None:
                continue
            
            # 检查新任务的 label 值是否在规则中定义
            if incoming_value in rules:
                # 获取该 label 值可以抢占的目标列表
                allowed_victims = rules[incoming_value]
                
                # 检查运行任务的 label 值是否在可抢占列表中
                if running_value in allowed_victims:
                    logger.debug(
                        "Label 抢占规则匹配: %s=%s 可以抢占 %s=%s",
                        label_key,
                        incoming_value,
                        label_key,
                        running_value,
                    )
                    return True
        
        # 没有任何规则匹配
        return False

    def _save_task_state(self, task_state: TaskState) -> Dict[str, Any]:
        """保存任务状态"""
        # 检查是否有自定义保存器
        if task_state.pool_name in self.state_handlers:
            preserver, _ = self.state_handlers[task_state.pool_name]
            try:
                custom_state = preserver(task_state)
                logger.debug("使用自定义处理器保存状态 pool=%s", task_state.pool_name)
                return custom_state
            except Exception as exc:
                logger.warning(
                    "自定义状态保存器失败，回退到默认策略 pool=%s error=%s",
                    task_state.pool_name,
                    exc,
                )

        # 默认保存策略：保存所有任务信息
        return self._task_state_to_dict(task_state)

    @staticmethod
    def _task_state_to_dict(task_state: TaskState) -> Dict[str, Any]:
        """将 TaskState 转换为字典"""
        result = {
            "task_id": task_state.task_id,
            "agent_name": task_state.agent_name,
            "pool_name": task_state.pool_name,
            "priority": task_state.priority,
            "labels": dict(task_state.labels),
            "start_time": task_state.start_time,
            "estimated_duration": task_state.estimated_duration,
            "payload": task_state.payload,
            "last_preemption_reason": task_state.last_preemption_reason,
        }
        if task_state.resources is not None:
            result["resources"] = task_state.resources.to_dict()
        return result

    @staticmethod
    def _candidate_to_dict(candidate: PreemptionCandidate) -> Dict[str, Any]:
        """将 PreemptionCandidate 转换为字典"""
        return {
            "task_id": candidate.task_state.task_id,
            "agent_name": candidate.task_state.agent_name,
            "pool_name": candidate.task_state.pool_name,
            "priority": candidate.task_state.priority,
            "preempt_score": candidate.preempt_score,
            "reason": candidate.reason,
        }

    def _policy_to_dict(self) -> Dict[str, Any]:
        """将策略转换为字典"""
        return {
            "same_pool_priority_threshold": self.policy.same_pool_priority_threshold,
            "cross_pool_priority_threshold": self.policy.cross_pool_priority_threshold,
            "remaining_time_weight": self.policy.remaining_time_weight,
            "min_preempt_score": self.policy.min_preempt_score,
            "enable_cross_pool_preemption": self.policy.enable_cross_pool_preemption,
            "protected_pools": list(self.policy.protected_pools),
            "label_preemption_rules": dict(self.policy.label_preemption_rules),
            "label_priority_threshold": self.policy.label_priority_threshold,
            "enable_label_preemption": self.policy.enable_label_preemption,
        }
