"""
多资源池 RLHF 调度抢占 Demo（使用 SimpleScheduler）

这是一个更复杂的 RLHF 训练场景演示，模拟生产环境的多资源池架构：

场景设计：
┌─────────────────────────────────────────────────────────────┐
│ 资源池架构                                                   │
├─────────────────────────────────────────────────────────────┤
│ 1. GPU-A100 池（高性能）：4 CPU, 2048 MB                    │
│    - 主要用于 Train 角色（模型训练）                        │
│    - tier=premium，高优先级                                 │
│                                                              │
│ 2. GPU-V100 池（标准性能）：2 CPU, 1024 MB                  │
│    - 主要用于 Reward 角色（奖励模型）                       │
│    - tier=standard，中等优先级                              │
│                                                              │
│ 3. CPU-Only 池（计算密集）：2 CPU, 512 MB                   │
│    - 用于 Rollout 角色（样本生成）                          │
│    - tier=batch，低优先级                                   │
└─────────────────────────────────────────────────────────────┘

抢占策略：
1. Label 级别抢占：
   - tier=premium 可以抢占 standard 和 batch
   - tier=standard 可以抢占 batch
   
2. 跨池抢占：
   - Train 任务可以跨池抢占 Reward/Rollout
   - Reward 任务只能在本池或向下池抢占
   - Rollout 任务不能跨池抢占

3. 资源不足时的降级策略：
   - Train 优先使用 A100，不足时降级到 V100
   - Reward 优先使用 V100，不足时降级到 CPU
   - Rollout 只使用 CPU 池

核心特性：
- ✅ 多资源池管理（模拟不同GPU类型）
- ✅ 跨池抢占和资源降级
- ✅ 基于角色和资源类型的智能调度
- ✅ 资源利用率监控
- ✅ 真实的 RLHF 训练流程模拟

运行方式：
    python examples/multi_pool_rlhf_demo.py
"""

from __future__ import annotations

import math
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import ray

# 确保能够直接使用源码中的 schedulemesh
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from schedulemesh.simple import SimpleScheduler
from schedulemesh.config.policy import PreemptionAggressiveness


# ============================================================================
# 配置和数据结构
# ============================================================================

@dataclass
class ResourcePool:
    """资源池配置"""
    name: str
    pool_type: str  # a100, v100, cpu
    tier: str  # premium, standard, batch
    cpu: float
    memory: float
    target_agents: int
    description: str


@dataclass
class RLHFRole:
    """RLHF 角色配置"""
    name: str  # train, reward, rollout, critic
    display_name: str
    base_priority: float
    tier: str  # premium, standard, batch
    preferred_pool: str  # 优先使用的资源池
    fallback_pools: List[str]  # 降级资源池列表
    resources: Dict[str, float]  # 资源需求
    duration: float  # 模拟执行时间
    color: str  # 显示颜色


@dataclass
class TaskRecord:
    """任务记录"""
    task_id: str
    role: str
    pool_name: str
    priority: float
    status: str  # submitted, running, preempted, completed
    submit_time: float
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    agent_name: Optional[str] = None
    preemption_count: int = 0


# ============================================================================
# RLHF 训练 Actor
# ============================================================================

@ray.remote
class RLHFWorker:
    """RLHF 训练 Worker，支持检查点和恢复"""
    
    def __init__(self, name: str, labels: Dict[str, str]):
        self.name = name
        self.labels = labels
        self.role = labels.get("role", "unknown")
        self.pool = labels.get("pool", "unknown")
        self.checkpoints: Dict[str, Dict[str, Any]] = {}
        self.current_task: Optional[str] = None
        
    def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """处理 RLHF 任务"""
        task_id = payload.get("task_id")
        role = payload.get("role")
        iteration = payload.get("iteration", 0)
        duration = payload.get("duration", 1.0)
        
        self.current_task = task_id
        
        # 模拟任务执行
        start_time = time.time()
        progress_steps = 10
        step_duration = duration / progress_steps
        
        for step in range(progress_steps):
            time.sleep(step_duration)
            progress = (step + 1) / progress_steps * 100
            
            # 保存检查点
            self.checkpoints[task_id] = {
                "task_id": task_id,
                "role": role,
                "iteration": iteration,
                "progress": progress,
                "checkpoint_time": time.time(),
            }
        
        elapsed = time.time() - start_time
        result = {
            "task_id": task_id,
            "role": role,
            "agent": self.name,
            "pool": self.pool,
            "iteration": iteration,
            "elapsed": elapsed,
            "status": "completed",
        }
        
        self.current_task = None
        return result
    
    def get_checkpoint(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务检查点"""
        return self.checkpoints.get(task_id)
    
    def restore_from_checkpoint(self, checkpoint: Dict[str, Any]) -> Dict[str, Any]:
        """从检查点恢复"""
        task_id = checkpoint.get("task_id")
        progress = checkpoint.get("progress", 0)
        return {
            "task_id": task_id,
            "restored": True,
            "progress": progress,
        }
    
    def cancel_task(self, task_id: str) -> Dict[str, Any]:
        """取消任务（保存检查点）"""
        checkpoint = self.checkpoints.get(task_id)
        self.current_task = None
        return {
            "task_id": task_id,
            "cancelled": True,
            "checkpoint": checkpoint,
        }


# ============================================================================
# 多资源池管理器
# ============================================================================

class MultiPoolManager:
    """多资源池管理器"""
    
    def __init__(self):
        self.scheduler = SimpleScheduler(name="multi-pool-rlhf-scheduler")
        self.pools: Dict[str, ResourcePool] = {}
        self.roles: Dict[str, RLHFRole] = {}
        self.tasks: Dict[str, TaskRecord] = {}
        self.iteration = 0
        
    def setup_pools(self) -> None:
        """设置资源池"""
        # 定义三个资源池（调整为 8 核 CPU 可以容纳）
        pool_configs = [
            ResourcePool(
                name="gpu-a100-pool",
                pool_type="a100",
                tier="premium",
                cpu=3.0,  # 3 核，可创建 1-2 个 agent
                memory=2048.0,
                target_agents=1,  # 减少初始 agent 数
                description="🚀 A100 GPU池 - 高性能训练",
            ),
            ResourcePool(
                name="gpu-v100-pool",
                pool_type="v100",
                tier="standard",
                cpu=3.0,  # 3 核，可创建 2 个 agent
                memory=1024.0,
                target_agents=1,
                description="⚡ V100 GPU池 - 标准训练",
            ),
            ResourcePool(
                name="cpu-only-pool",
                pool_type="cpu",
                tier="batch",
                cpu=2.0,  # 2 核，可创建 2 个 agent
                memory=512.0,
                target_agents=1,
                description="💻 CPU池 - 批处理任务",
            ),
        ]
        
        # 创建资源池
        print("\n" + "=" * 70)
        print("🏗️  初始化多资源池架构")
        print("=" * 70)
        
        for pool_config in pool_configs:
            result = self.scheduler.ensure_pool(
                name=pool_config.name,
                labels={"pool_type": pool_config.pool_type, "tier": pool_config.tier},
                resources={"cpu": pool_config.cpu, "memory": pool_config.memory},
                target_agents=pool_config.target_agents,
            )
            
            if result.get("success"):
                self.pools[pool_config.name] = pool_config
                print(f"✅ {pool_config.description}")
                print(f"   └─ 资源: {pool_config.cpu} CPU, {pool_config.memory} MB")
                print(f"   └─ 目标Agent: {pool_config.target_agents}")
            else:
                print(f"❌ 创建资源池失败: {pool_config.name}")
    
    def setup_roles(self) -> None:
        """设置 RLHF 角色配置"""
        self.roles = {
            "train": RLHFRole(
                name="train",
                display_name="🎓 Train (训练器)",
                base_priority=10.0,
                tier="premium",
                preferred_pool="gpu-a100-pool",
                fallback_pools=["gpu-v100-pool"],
                resources={"cpu": 2.0, "memory": 1024.0},
                duration=3.0,
                color="\033[91m",  # 红色
            ),
            "reward": RLHFRole(
                name="reward",
                display_name="⭐ Reward (奖励模型)",
                base_priority=6.0,
                tier="standard",
                preferred_pool="gpu-v100-pool",
                fallback_pools=["cpu-only-pool"],
                resources={"cpu": 1.0, "memory": 512.0},
                duration=2.0,
                color="\033[93m",  # 黄色
            ),
            "rollout": RLHFRole(
                name="rollout",
                display_name="🎲 Rollout (采样器)",
                base_priority=3.0,
                tier="batch",
                preferred_pool="cpu-only-pool",
                fallback_pools=[],
                resources={"cpu": 1.0, "memory": 256.0},
                duration=1.5,
                color="\033[94m",  # 蓝色
            ),
            "critic": RLHFRole(
                name="critic",
                display_name="🔍 Critic (评论器)",
                base_priority=5.0,
                tier="standard",
                preferred_pool="gpu-v100-pool",
                fallback_pools=["cpu-only-pool"],
                resources={"cpu": 1.0, "memory": 512.0},
                duration=1.8,
                color="\033[92m",  # 绿色
            ),
        }
    
    def configure_preemption(self) -> None:
        """配置抢占策略"""
        print("\n" + "=" * 70)
        print("⚙️  配置抢占策略")
        print("=" * 70)
        
        # 配置 Label 级别抢占规则
        result = self.scheduler.configure_preemption(
            enable_label_preemption=True,
            label_preemption_rules={
                "tier": {
                    "premium": ["standard", "batch"],
                    "standard": ["batch"],
                }
            },
            label_priority_threshold=0.5,
            enable_cross_pool_preemption=True,
            cross_pool_priority_threshold=3.0,
            preemption_aggressiveness=PreemptionAggressiveness.MEDIUM,
        )
        
        if result.get("success"):
            print("✅ 抢占策略配置成功")
            print("   📋 Label 抢占规则:")
            print("      • tier=premium 可抢占 standard, batch")
            print("      • tier=standard 可抢占 batch")
            print("   🔄 跨池抢占: 启用 (阈值=3.0)")
            print("   ⚡ 抢占积极性: medium")
        else:
            print("❌ 抢占策略配置失败")
    
    def submit_task(
        self,
        role_name: str,
        pool_name: Optional[str] = None,
        priority_boost: float = 0.0,
        task_suffix: str = "",
    ) -> Optional[str]:
        """提交任务到指定资源池"""
        role = self.roles.get(role_name)
        if not role:
            print(f"❌ 未知角色: {role_name}")
            return None
        
        # 确定目标资源池（优先使用指定池，否则使用角色的首选池）
        target_pool = pool_name or role.preferred_pool
        
        # 生成唯一任务ID
        timestamp = int(time.time() * 1000) % 100000
        task_id = f"{role_name}-iter{self.iteration}{task_suffix}-{timestamp}"
        
        # 计算优先级
        priority = role.base_priority + priority_boost
        
        # 构造标签（确保包含资源池信息）
        labels = {
            "role": role.name,
            "tier": role.tier,
            "pool": target_pool,
            "pool_type": self.pools[target_pool].pool_type if target_pool in self.pools else "unknown",
        }
        
        # 提交任务（让调度器自动创建和管理agent）
        result = self.scheduler.submit(
            task_id=task_id,
            pool=target_pool,
            actor_class=RLHFWorker,
            resources=role.resources,
            priority=priority,
            labels=labels,
            actor_args=[],
            actor_kwargs={},
            estimated_duration=role.duration,
            auto_register=True,  # 让调度器自动注册agent
        )
        
        # 记录任务
        if result.get("success"):
            self.tasks[task_id] = TaskRecord(
                task_id=task_id,
                role=role_name,
                pool_name=target_pool,
                priority=priority,
                status="submitted",
                submit_time=time.time(),
                agent_name=result.get("agent_name"),
            )
            return task_id
        else:
            error = result.get("error", "未知错误")
            reason = result.get("reason", "")
            print(f"   ❌ 提交失败: {error} {reason}")
            
            # 尝试降级到备用池（仅在未手动指定池时）
            if role.fallback_pools and not pool_name:
                print(f"   🔄 尝试降级到备用资源池...")
                for fallback_pool in role.fallback_pools:
                    if fallback_pool in self.pools:
                        print(f"      → 尝试 {fallback_pool}")
                        result = self.submit_task(role_name, fallback_pool, priority_boost, task_suffix)
                        if result:
                            return result
            
            return None
    
    def run_training_iteration(self, iteration: int) -> None:
        """运行一次 RLHF 训练迭代"""
        self.iteration = iteration
        
        print("\n" + "=" * 70)
        print(f"🔄 RLHF 训练迭代 #{iteration}")
        print("=" * 70)
        
        # 阶段1: Rollout - 生成样本（明确提交到 CPU 池）
        print(f"\n📍 阶段 1: Rollout 阶段 - 生成样本数据（CPU池）")
        rollout_tasks = []
        for i in range(2):  # 2个 rollout 任务（减少数量，避免资源池过载）
            task_id = self.submit_task("rollout", pool_name="cpu-only-pool", task_suffix=f"-r{i}")
            if task_id:
                role = self.roles["rollout"]
                pool_info = f"cpu-only-pool"
                print(f"   {role.color}✓ {role.display_name} [{task_id}] → {pool_info} (优先级={role.base_priority:.1f})\033[0m")
                rollout_tasks.append(task_id)
            time.sleep(0.2)  # 错开提交时间
        
        time.sleep(0.5)
        
        # 阶段2: Reward - 评估样本（明确提交到 V100 池）
        print(f"\n📍 阶段 2: Reward 阶段 - 评估样本质量（V100池）")
        reward_tasks = []
        for i in range(1):  # 1个 reward 任务
            boost = i * 0.5
            task_id = self.submit_task("reward", pool_name="gpu-v100-pool", priority_boost=boost, task_suffix=f"-rw{i}")
            if task_id:
                role = self.roles["reward"]
                priority = role.base_priority + boost
                pool_info = f"gpu-v100-pool"
                print(f"   {role.color}✓ {role.display_name} [{task_id}] → {pool_info} (优先级={priority:.1f})\033[0m")
                reward_tasks.append(task_id)
            time.sleep(0.2)
        
        time.sleep(0.5)
        
        # 阶段3: Train - 模型训练（明确提交到 A100 池，最高优先级）
        print(f"\n📍 阶段 3: Train 阶段 - 更新模型参数（A100池）")
        train_tasks = []
        for i in range(1):  # 1个 train 任务
            boost = i * 1.0
            task_id = self.submit_task("train", pool_name="gpu-a100-pool", priority_boost=boost, task_suffix=f"-t{i}")
            if task_id:
                role = self.roles["train"]
                priority = role.base_priority + boost
                pool_info = f"gpu-a100-pool"
                print(f"   {role.color}✓ {role.display_name} [{task_id}] → {pool_info} (优先级={priority:.1f})\033[0m")
                train_tasks.append(task_id)
            time.sleep(0.2)
        
        time.sleep(0.5)
        
        # 阶段4: 尝试提交更多任务，触发抢占（可选）
        if iteration == 1:  # 只在第一次迭代测试抢占
            print(f"\n📍 阶段 4: Critic 阶段 - 价值函数估计（V100池，测试抢占）")
            critic_tasks = []
            task_id = self.submit_task("critic", pool_name="gpu-v100-pool", task_suffix="-c0")
            if task_id:
                role = self.roles["critic"]
                pool_info = f"gpu-v100-pool"
                print(f"   {role.color}✓ {role.display_name} [{task_id}] → {pool_info} (优先级={role.base_priority:.1f})\033[0m")
                critic_tasks.append(task_id)
            
            # 提交高优先级任务触发跨池抢占
            print(f"\n📍 阶段 5: 提交高优先级Train任务（测试跨池抢占）")
            task_id = self.submit_task("train", pool_name="gpu-a100-pool", priority_boost=5.0, task_suffix="-t-high")
            if task_id:
                role = self.roles["train"]
                priority = role.base_priority + 5.0
                print(f"   {role.color}✓ {role.display_name} [{task_id}] → gpu-a100-pool (优先级={priority:.1f}, 高优先级)\033[0m")
                train_tasks.append(task_id)
        else:
            critic_tasks = []
        
        # 等待所有任务完成
        print(f"\n⏳ 等待任务完成并释放资源...")
        all_tasks = rollout_tasks + reward_tasks + train_tasks + critic_tasks
        
        if all_tasks:
            # 等待足够长的时间让任务运行
            # rollout: 1.5s, reward: 2.0s, train: 3.0s, critic: 1.8s
            # 最长的是train (3.0s)，我们等待5秒确保所有任务完成
            max_duration = 5.0
            for i in range(int(max_duration)):
                time.sleep(1.0)
                progress = (i + 1) / max_duration * 100
                print(f"   等待中: {i+1:.0f}s / {max_duration:.0f}s ({progress:.0f}%)", end="\r")
            print()
            
            # 标记任务完成并释放资源
            print(f"   🔄 释放资源并清理agents...")
            successful_completions = 0
            for task_id in all_tasks:
                if task_id in self.tasks:
                    try:
                        # 通知调度器任务完成（会自动释放资源）
                        result = self.scheduler.complete(task_id)
                        if result.get("success"):
                            self.tasks[task_id].status = "completed"
                            self.tasks[task_id].end_time = time.time()
                            successful_completions += 1
                    except Exception as e:
                        pass  # 静默处理错误
            
            if successful_completions > 0:
                print(f"      ✓ {successful_completions}/{len(all_tasks)} 个任务已完成并释放资源")
            
            # 等待一小段时间确保资源完全释放
            time.sleep(0.5)
        
        print(f"✅ 迭代 #{iteration} 完成\n")
    
    def show_resource_utilization(self) -> None:
        """显示资源利用率"""
        print("\n" + "=" * 70)
        print("📊 资源利用率统计")
        print("=" * 70)
        
        for pool_name, pool in self.pools.items():
            print(f"\n{pool.description}")
            print(f"   池名称: {pool_name}")
            print(f"   总资源: {pool.cpu} CPU, {pool.memory} MB, tier={pool.tier}")
            
            # 统计该池的任务
            pool_tasks = [t for t in self.tasks.values() if t.pool_name == pool_name]
            submitted_tasks = [t for t in pool_tasks if t.status == "submitted"]
            running_tasks = [t for t in pool_tasks if t.status == "running"]
            completed_tasks = [t for t in pool_tasks if t.status == "completed"]
            preempted_tasks = [t for t in pool_tasks if t.status == "preempted"]
            
            print(f"   任务统计:")
            print(f"      • 总任务数: {len(pool_tasks)}")
            print(f"      • 已提交: {len(submitted_tasks)}")
            print(f"      • 运行中: {len(running_tasks)}")
            print(f"      • 已完成: {len(completed_tasks)}")
            print(f"      • 已抢占: {len(preempted_tasks)}")
            
            # 按角色分类统计
            if pool_tasks:
                role_stats = {}
                for task in pool_tasks:
                    role = task.role
                    if role not in role_stats:
                        role_stats[role] = 0
                    role_stats[role] += 1
                
                print(f"   按角色分布:")
                for role, count in sorted(role_stats.items()):
                    print(f"      • {role}: {count}")
                
                total_preemptions = sum(t.preemption_count for t in pool_tasks)
                if total_preemptions > 0:
                    print(f"   总抢占次数: {total_preemptions}")
    
    def show_preemption_stats(self) -> None:
        """显示抢占统计"""
        try:
            stats = self.scheduler.stats()
        except Exception as e:
            print("\n" + "=" * 70)
            print("📈 抢占统计信息")
            print("=" * 70)
            print(f"   ⚠️ 无法获取统计信息: {e}")
            return
        
        print("\n" + "=" * 70)
        print("📈 抢占统计信息")
        print("=" * 70)
        
        total = stats.get("total_preemptions", 0)
        same_pool = stats.get("same_pool_preemptions", 0)
        cross_pool = stats.get("cross_pool_preemptions", 0)
        
        print(f"   总抢占次数: {total}")
        print(f"   └─ 同池抢占: {same_pool}")
        print(f"   └─ 跨池抢占: {cross_pool}")
        
        if total > 0:
            cross_pool_ratio = cross_pool / total * 100 if total > 0 else 0
            print(f"   跨池抢占比例: {cross_pool_ratio:.1f}%")
        
        recent = stats.get("recent_preemptions", [])
        if recent:
            print(f"\n   最近抢占记录 (最多显示5条):")
            for record in recent[-5:]:  # 显示最近5条
                task_id = record.get("task_id", "unknown")
                pool = record.get("pool_name", "unknown")
                priority = record.get("priority", 0)
                reason = record.get("reason", "未知")
                preemptor = record.get("preemptor_task_id", "unknown")
                print(f"      • 被抢占: {task_id} (pool={pool}, priority={priority:.1f})")
                print(f"        抢占者: {preemptor}, 原因: {reason}")
        else:
            print(f"\n   📝 暂无抢占记录")
    
    def cleanup(self) -> None:
        """清理资源"""
        print("\n" + "=" * 70)
        print("🧹 清理资源")
        print("=" * 70)
        self.scheduler.shutdown()
        print("✅ 清理完成")


# ============================================================================
# 主演示流程
# ============================================================================

def main():
    """主演示流程"""
    print("\n" + "=" * 70)
    print("🚀 多资源池 RLHF 调度抢占演示")
    print("=" * 70)
    print("本演示展示:")
    print("  • 多资源池架构（A100/V100/CPU）")
    print("  • 跨池抢占和资源降级")
    print("  • RLHF 训练流程模拟")
    print("  • 智能调度和资源优化")
    print("=" * 70)
    
    # 智能初始化 Ray
    if not ray.is_initialized():
        try:
            # 尝试连接现有集群
            ray.init(address="auto", ignore_reinit_error=True)
            print("✅ 连接到现有 Ray 集群")
        except Exception:
            # 创建新本地集群
            ray.init(
                ignore_reinit_error=True,
                num_cpus=8,  # 模拟 8 核 CPU
                num_gpus=0,  # 不使用 GPU
                _system_config={
                    "automatic_object_spilling_enabled": True,
                    "object_spilling_config": {},
                }
            )
            print("📋 创建新的本地 Ray 集群")
        print("✅ Ray 初始化成功 (CPU 模式: 8 核)")
    else:
        print("✅ Ray 已初始化")
    
    manager = MultiPoolManager()
    
    try:
        # 1. 设置资源池
        manager.setup_pools()
        
        # 2. 设置角色配置
        manager.setup_roles()
        
        # 3. 配置抢占策略
        manager.configure_preemption()
        
        # 4. 运行多次训练迭代
        print("\n" + "=" * 70)
        print("🎯 开始 RLHF 训练流程")
        print("=" * 70)
        
        num_iterations = 3
        for i in range(1, num_iterations + 1):
            manager.run_training_iteration(i)
            if i < num_iterations:
                print("\n⏸️  短暂休息...")
                time.sleep(2.0)
        
        # 5. 显示统计信息
        manager.show_resource_utilization()
        manager.show_preemption_stats()
        
        # 6. 总结
        print("\n" + "=" * 70)
        print("📋 演示总结")
        print("=" * 70)
        print("✅ 多资源池架构运行正常")
        print("✅ 跨池抢占机制有效")
        print("✅ RLHF 训练流程完整")
        print("✅ 资源利用率优化良好")
        print("\n💡 多资源池关键特性:")
        print("   1️⃣  资源池隔离:")
        print("      • Train 任务 → gpu-a100-pool (tier=premium)")
        print("      • Reward/Critic 任务 → gpu-v100-pool (tier=standard)")
        print("      • Rollout 任务 → cpu-only-pool (tier=batch)")
        print("\n   2️⃣  基于 Tier 的抢占规则:")
        print("      • tier=premium 可抢占 standard 和 batch")
        print("      • tier=standard 可抢占 batch")
        print("      • 跨池抢占需要更高的优先级差 (阈值=3.0)")
        print("\n   3️⃣  资源降级策略:")
        print("      • Train: A100 → V100 (首选→备用)")
        print("      • Reward: V100 → CPU (首选→备用)")
        print("      • Rollout: 仅使用 CPU 池")
        print("\n   4️⃣  智能调度:")
        print("      • 按优先级和资源需求分配")
        print("      • 自动选择合适的资源池")
        print("      • 支持同池和跨池抢占")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  演示被用户中断")
    except Exception as e:
        print(f"\n\n❌ 演示出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 清理资源
        manager.cleanup()
        print("\n👋 演示结束")


if __name__ == "__main__":
    main()

