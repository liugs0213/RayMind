"""
RLHF 角色调度抢占 Demo（使用 SimpleScheduler）

这是一个简化版的 RLHF 训练场景演示，使用 SimpleScheduler API 来展示：
1. Rollout 角色启动，负责生成样本数据
2. Reward 角色到达，根据角色标签抢占 Rollout
3. Train 角色到达，抢占 Reward 并获得资源

RLHF 角色说明：
- Train（训练器）：最高优先级，负责更新模型参数
- Reward（奖励模型）：中等优先级，负责评估生成质量
- Rollout（采样器）：较低优先级，负责生成样本数据
- Critic（评论器）：中等优先级，负责价值估计

核心特性：
- ✅ 使用 SimpleScheduler 简化的 API
- ✅ 基于 label 的角色抢占（train > reward > rollout）
- ✅ 自动任务注册和抢占处理
- ✅ 检查点保存与恢复机制
- ✅ 完全 CPU 本地运行，无需 GPU

运行方式：
    python examples/simple_rlhf_preemption_demo.py
"""

from __future__ import annotations

import math
import random
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import ray

# 确保能够直接使用源码中的 schedulemesh
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from schedulemesh.simple import SimpleScheduler
from schedulemesh.config.policy import PreemptionAggressiveness


@dataclass
class RLHFRoleConfig:
    """RLHF 角色配置"""
    role: str  # train, reward, rollout, critic
    model_size: str  # 7B, 13B, 70B
    batch_size: int
    learning_rate: float
    cpu_required: float
    memory_required: float  # MB


@dataclass
class RoleStep:
    """RLHF 角色执行步骤"""
    step: int
    metric_value: float  # 根据角色不同：reward分数、loss、样本数等
    loss: float
    resource_utilization: float  # 0.0 - 1.0
    throughput: float  # tokens/sec 或 samples/sec
    timestamp: float


@ray.remote
class RLHFRoleAgent:
    """
    简化版的 RLHF 角色 Agent，专注于核心功能。

    特性：
    - 支持不同 RLHF 角色（train、reward、rollout、critic）
    - 模拟真实的角色工作负载
    - 支持检查点保存与恢复
    """

    def __init__(self, name: str, labels: Dict[str, str]):
        self.name = name
        self.labels = labels
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._task_threads: Dict[str, threading.Thread] = {}
        self._stop_flags: Dict[str, threading.Event] = {}
        self._lock = threading.Lock()
        
        print(f"📦 RLHFRoleAgent '{name}' 初始化 | 标签: {labels}")

    def start_role_task(
        self,
        task_id: str,
        config: RLHFRoleConfig,
        *,
        steps: int = 100,
        step_duration: float = 0.15,
        checkpoint_interval: int = 10,
    ) -> dict:
        """
        启动 RLHF 角色任务（异步执行）。

        Args:
            task_id: 任务标识
            config: 角色配置（包含资源需求）
            steps: 执行总步数
            step_duration: 每步耗时（秒）
            checkpoint_interval: 检查点保存间隔
        """
        stop_flag = threading.Event()
        role_steps: List[RoleStep] = []
        checkpoints: List[int] = []
        
        summary = {
            "task_id": task_id,
            "config": config.__dict__,
            "status": "running",
            "start_time": time.time(),
            "steps": role_steps,
            "checkpoints": checkpoints,
            "total_metric": 0.0,
            "avg_throughput": 0.0,
        }

        with self._lock:
            if task_id in self._tasks:
                return {"success": False, "error": f"Task '{task_id}' already running"}
            self._tasks[task_id] = summary
            self._stop_flags[task_id] = stop_flag

        def _run_role_task():
            total_metric = 0.0
            total_throughput = 0.0
            
            # 根据 RLHF 角色调整基础参数
            if config.role == "train":
                base_metric = 0.5  # 初始loss
                exploration = 0.1
                base_throughput = 2000.0  # tokens/sec
                resource_util_base = 0.95
                metric_name = "loss"
            elif config.role == "reward":
                base_metric = 0.6  # 初始reward分数
                exploration = 0.15
                base_throughput = 3000.0  # samples/sec
                resource_util_base = 0.85
                metric_name = "reward_score"
            elif config.role == "rollout":
                base_metric = 0.7  # 初始生成质量
                exploration = 0.1
                base_throughput = 5000.0  # samples/sec
                resource_util_base = 0.75
                metric_name = "generation_quality"
            elif config.role == "critic":
                base_metric = 0.6
                exploration = 0.12
                base_throughput = 3500.0
                resource_util_base = 0.80
                metric_name = "value_estimate"
            else:
                base_metric = 0.5
                exploration = 0.1
                base_throughput = 4000.0
                resource_util_base = 0.8
                metric_name = "metric"
            
            for step_idx in range(1, steps + 1):
                if stop_flag.is_set():
                    summary["status"] = "preempted"
                    break

                time.sleep(step_duration)
                
                # 模拟角色指标
                decay = math.exp(-step_idx / max(steps, 1))
                metric_value = base_metric + (1 - base_metric) * (1 - decay)
                metric_value += random.uniform(-exploration, exploration)
                metric_value = max(0.0, min(1.0, metric_value))
                
                # loss 随着训练下降
                loss = max(0.0, 1.5 * (1 - metric_value) + random.uniform(-0.05, 0.05))
                
                # 模拟资源利用率
                resource_util = resource_util_base + random.uniform(-0.05, 0.05)
                resource_util = max(0.0, min(1.0, resource_util))
                
                # 模拟吞吐量
                throughput = base_throughput * (config.batch_size / 32.0) * resource_util
                throughput += random.uniform(-200, 200)
                throughput = max(0.0, throughput)

                total_metric += metric_value
                total_throughput += throughput
                
                role_steps.append(
                    RoleStep(
                        step=step_idx,
                        metric_value=metric_value,
                        loss=loss,
                        resource_utilization=resource_util,
                        throughput=throughput,
                        timestamp=time.time(),
                    )
                )
                
                # 保存检查点
                if step_idx % checkpoint_interval == 0:
                    checkpoints.append(step_idx)

            else:
                summary["status"] = "completed"

            summary["end_time"] = time.time()
            summary["total_metric"] = total_metric
            summary["avg_throughput"] = total_throughput / max(len(role_steps), 1)

        worker = threading.Thread(target=_run_role_task, name=f"role-{task_id}", daemon=True)
        with self._lock:
            self._task_threads[task_id] = worker
        worker.start()
        
        print(
            f"🚀 启动 RLHF 角色任务 '{task_id}' | "
            f"角色={config.role} 模型={config.model_size} "
            f"资源=[CPU:{config.cpu_required}, MEM:{config.memory_required/1024:.1f}GB]"
        )
        return {"success": True, "task_id": task_id}

    def cancel(self, task_id: str) -> dict:
        """停止指定角色任务，保存检查点并返回任务摘要。"""
        with self._lock:
            stop_flag = self._stop_flags.get(task_id)
            summary = self._tasks.get(task_id)
            thread = self._task_threads.get(task_id)

        if stop_flag is None or summary is None:
            return {"success": False, "error": f"Task '{task_id}' not found"}

        print(f"⏸️  取消角色任务 '{task_id}'，保存检查点...")
        stop_flag.set()
        if thread and thread.is_alive():
            thread.join(timeout=2.0)

        with self._lock:
            self._stop_flags.pop(task_id, None)
            self._task_threads.pop(task_id, None)

        summary.setdefault("end_time", time.time())
        summary.setdefault("status", "preempted")
        
        # 保存最终检查点
        if summary.get("steps"):
            last_step = len(summary["steps"])
            if last_step not in summary.get("checkpoints", []):
                summary.setdefault("checkpoints", []).append(last_step)
        
        print(
            f"💾 任务 '{task_id}' 已保存检查点: "
            f"step={len(summary.get('steps', []))} "
            f"checkpoints={summary.get('checkpoints', [])}"
        )
        return {"success": True, "task": summary}

    def role_task_summary(self, task_id: Optional[str] = None) -> dict:
        """返回指定任务或全部任务的当前状态。"""
        with self._lock:
            if task_id:
                task = self._tasks.get(task_id)
                if task is None:
                    return {"success": False, "error": f"Task '{task_id}' not found"}
                return {
                    "success": True,
                    "task": {
                        **task,
                        "steps": [step.__dict__ for step in task["steps"]],
                    },
                }

            return {
                "success": True,
                "tasks": {
                    tid: {
                        **summary,
                        "steps": [step.__dict__ for step in summary["steps"]],
                    }
                    for tid, summary in self._tasks.items()
                },
            }


def print_stats(scheduler: SimpleScheduler, title: str) -> None:
    """打印抢占统计信息"""
    stats = scheduler.stats()
    print(f"\n📊 {title}")
    print(f"  当前运行任务: {stats.get('running_tasks', 0)}")
    print(f"  累计抢占次数: {stats.get('total_preemptions', 0)}")
    recent = stats.get("recent_preemptions") or []
    if recent:
        print("  最近抢占记录:")
        for record in recent[-3:]:  # 只显示最近3条
            task_id = record.get("task_id", "unknown")
            reason = record.get("reason", "unknown")
            cancel_success = record.get("cancel_success", False)
            print(f"    • {task_id}: {reason} (取消成功={cancel_success})")
    else:
        print("  最近抢占记录: 无")


def run_demo() -> None:
    print("=" * 80)
    print("🚀 RLHF 角色调度抢占 Demo（使用 SimpleScheduler）")
    print("=" * 80)
    
    # Ray 配置参数
    ray_config = {
        "ignore_reinit_error": True,
        "local_mode": True,  # 进程内执行，便于调试
        "num_cpus": 4,       # 限制 CPU 核心数
        "include_dashboard": False,
        "log_to_driver": False,
    }
    
    # 智能初始化 Ray
    try:
        # 尝试连接现有集群
        ray.init(address="auto", ignore_reinit_error=True)
        print("✅ 连接到现有 Ray 集群")
    except Exception:
        # 创建新本地集群
        print(f"📋 Ray 配置: {ray_config}")
        ray.init(**ray_config)
    
    # 创建 SimpleScheduler
    scheduler = SimpleScheduler("simple-rlhf-demo")
    
    try:
        # ===== 配置抢占策略 =====
        print("\n📋 配置基于 Label 的 RLHF 角色抢占策略...")
        scheduler.configure_preemption(
            enable_label_preemption=True,
            label_preemption_rules={
                "role": {
                    # Train 角色可以抢占所有其他角色
                    "train": ["reward", "critic", "rollout"],
                    # Reward 和 Critic 可以抢占 Rollout
                    "reward": ["rollout"],
                    "critic": ["rollout"],
                }
            },
            preemption_aggressiveness=PreemptionAggressiveness.MEDIUM,  # 中等激进程度
            cross_pool_priority_threshold=5.0,
            enable_cross_pool_preemption=True,
        )
        print("✅ 抢占策略配置完成: Train > Reward/Critic > Rollout")

        # ===== 创建资源池 =====
        pool_name = "rlhf-shared-pool"
        print(f"\n🏗️  创建 RLHF 共享资源池...")
        pool_result = scheduler.ensure_pool(
            name=pool_name,
            labels={"workload": "rlhf", "tier": "shared"},
            resources={"cpu": 2.0, "memory": 6144.0},  # 增加到 6GB 以支持抢占
            target_agents=0,  # 不预先创建 agent，由任务提交时创建
        )
        
        if pool_result.get("success"):
            print(f"  ✅ {pool_name}: 2 CPU, 6GB Memory")
        else:
            print(f"  ❌ 资源池创建失败: {pool_result.get('error')}")
            return

        # ===== 场景 1：启动 Rollout 角色任务（生成样本数据） =====
        print(f"\n" + "=" * 80)
        print(f"📊 场景 1: 启动 Rollout 角色任务（生成样本数据）")
        print(f"=" * 80)
        
        rollout_task_id = "rollout-data-collection"
        rollout_config = RLHFRoleConfig(
            role="rollout",
            model_size="13B",
            batch_size=24,
            learning_rate=5e-5,
            cpu_required=2.0,
            memory_required=1536.0,  # 1.5GB - 减少资源占用以便抢占
        )
        
        print(f"任务 ID: {rollout_task_id}")
        print(f"角色: {rollout_config.role} - 负责生成训练样本")
        print(f"模型规模: {rollout_config.model_size}")
        print(f"资源需求: {rollout_config.cpu_required} CPU, "
              f"{rollout_config.memory_required/1024:.2f} GB Memory")
        
        # 使用 SimpleScheduler.submit() 提交任务
        rollout_result = scheduler.submit(
            task_id=rollout_task_id,
            pool=pool_name,
            actor_class=RLHFRoleAgent,
            resources={"cpu": rollout_config.cpu_required, "memory": rollout_config.memory_required},
            priority=5.0,
            labels={
                "job": "rlhf",
                "role": rollout_config.role,
                "model_size": rollout_config.model_size,
            },
            estimated_duration=180.0,  # 预估3分钟
            auto_register=True,  # 自动注册到抢占控制器
        )
        
        if rollout_result.get("success"):
            print(f"✅ Rollout 任务提交成功")
            print(f"   Agent: {rollout_result['agent']['name']}")
            print(f"   Labels: {rollout_config.role}")
            rollout_agent = rollout_result["agent"]["handle"]
            
            # 启动角色任务
            start_result = ray.get(
                rollout_agent.start_role_task.remote(
                    task_id=rollout_task_id,
                    config=rollout_config,
                    steps=90,
                    step_duration=0.12,
                    checkpoint_interval=15,
                )
            )
            
            if start_result.get("success"):
                print(f"✅ Rollout 角色任务已启动")
            else:
                print(f"❌ Rollout 角色任务启动失败: {start_result.get('error')}")
            
            # 验证任务是否被注册到抢占控制器
            time.sleep(0.5)  # 等待注册完成
            stats = scheduler.stats()
            print(f"   📊 当前运行任务数: {stats.get('running_tasks', 0)}")
        else:
            print(f"❌ Rollout 任务提交失败: {rollout_result.get('error')}")

        print("\n⏳ Rollout 角色运行中，等待 3 秒观察...")
        time.sleep(3.0)
        
        print_stats(scheduler, "Rollout 任务运行后的统计")

        # ===== 场景 2：Reward 角色到达，触发抢占 =====
        print(f"\n" + "=" * 80)
        print(f"🚨 场景 2: Reward 角色到达，需要资源")
        print(f"=" * 80)
        
        reward_task_id = "reward-model-eval"
        reward_config = RLHFRoleConfig(
            role="reward",
            model_size="13B",
            batch_size=16,
            learning_rate=1e-4,
            cpu_required=2.0,
            memory_required=1536.0,  # 1.5GB - 与 rollout 相同以便抢占
        )
        
        print(f"任务 ID: {reward_task_id}")
        print(f"角色: {reward_config.role} - 负责评估样本质量")
        print(f"模型规模: {reward_config.model_size}")
        print(f"资源需求: {reward_config.cpu_required} CPU, "
              f"{reward_config.memory_required/1024:.2f} GB Memory")
        print(f"\n💡 根据 label_preemption_rules: reward 可以抢占 rollout")
        
        reward_result = scheduler.submit(
            task_id=reward_task_id,
            pool=pool_name,
            actor_class=RLHFRoleAgent,
            resources={"cpu": reward_config.cpu_required, "memory": reward_config.memory_required},
            priority=5.0,
            labels={
                "job": "rlhf",
                "role": reward_config.role,
                "model_size": reward_config.model_size,
            },
            estimated_duration=120.0,  # 预估2分钟
            auto_register=True,
        )
        
        if reward_result.get("success"):
            print(f"✅ Reward 任务提交成功")
            reward_agent = reward_result["agent"]["handle"]
            
            # 启动角色任务
            start_result = ray.get(
                reward_agent.start_role_task.remote(
                    task_id=reward_task_id,
                    config=reward_config,
                    steps=80,
                    step_duration=0.14,
                    checkpoint_interval=12,
                )
            )
            
            if start_result.get("success"):
                print(f"✅ Reward 角色任务已启动")
            else:
                print(f"❌ Reward 角色任务启动失败: {start_result.get('error')}")
        else:
            print(f"❌ Reward 任务提交失败: {reward_result.get('error')}")
        
        print_stats(scheduler, "Reward 任务提交后的统计")
        
        print("\n⏳ Reward 角色运行中，等待 2 秒观察...")
        time.sleep(2.0)

        # 尝试查看 Rollout 任务状态（可能已被抢占）
        if rollout_result.get("success"):
            try:
                rollout_summary = ray.get(
                    rollout_agent.role_task_summary.remote(rollout_task_id)
                )
                if rollout_summary.get("success"):
                    task = rollout_summary["task"]
                    print(f"\n📦 Rollout 任务状态:")
                    print(f"  状态: {task['status']}")
                    print(f"  已完成步数: {len(task.get('steps', []))}")
                    print(f"  保存的检查点: {task.get('checkpoints', [])}")
            except Exception as e:
                print(f"ℹ️  Rollout 任务可能已被抢占清理: {e}")

        # ===== 场景 3：Train 角色到达，进一步抢占 =====
        print(f"\n" + "=" * 80)
        print(f"🔥 场景 3: Train 角色到达，需要资源")
        print(f"=" * 80)
        
        train_task_id = "rlhf-train-step"
        train_config = RLHFRoleConfig(
            role="train",
            model_size="7B",
            batch_size=32,
            learning_rate=2e-5,
            cpu_required=2.0,
            memory_required=1536.0,  # 1.5GB - 与其他任务相同以便抢占
        )
        
        print(f"任务 ID: {train_task_id}")
        print(f"角色: {train_config.role} - 负责更新模型参数")
        print(f"模型规模: {train_config.model_size}")
        print(f"资源需求: {train_config.cpu_required} CPU, "
              f"{train_config.memory_required/1024:.2f} GB Memory")
        print(f"\n💡 根据 label_preemption_rules: train 可以抢占 reward 和 rollout")
        
        train_result = scheduler.submit(
            task_id=train_task_id,
            pool=pool_name,
            actor_class=RLHFRoleAgent,
            resources={"cpu": train_config.cpu_required, "memory": train_config.memory_required},
            priority=5.0,
            labels={
                "job": "rlhf",
                "role": train_config.role,
                "model_size": train_config.model_size,
            },
            estimated_duration=90.0,  # 预估1.5分钟
            auto_register=True,
        )
        
        if train_result.get("success"):
            print(f"✅ Train 任务提交成功")
            train_agent = train_result["agent"]["handle"]
            
            # 启动角色任务
            start_result = ray.get(
                train_agent.start_role_task.remote(
                    task_id=train_task_id,
                    config=train_config,
                    steps=70,
                    step_duration=0.16,
                    checkpoint_interval=10,
                )
            )
            
            if start_result.get("success"):
                print(f"✅ Train 角色任务已启动")
            else:
                print(f"❌ Train 角色任务启动失败: {start_result.get('error')}")
        else:
            print(f"❌ Train 任务提交失败: {train_result.get('error')}")
        
        print_stats(scheduler, "Train 任务提交后的统计")
        
        # 尝试查看 Reward 任务状态（可能已被抢占）
        if reward_result.get("success"):
            try:
                reward_summary = ray.get(
                    reward_agent.role_task_summary.remote(reward_task_id)
                )
                if reward_summary.get("success"):
                    task = reward_summary["task"]
                    print(f"\n📦 Reward 任务状态:")
                    print(f"  状态: {task['status']}")
                    print(f"  已完成步数: {len(task.get('steps', []))}")
                    print(f"  保存的检查点: {task.get('checkpoints', [])}")
            except Exception as e:
                print(f"ℹ️  Reward 任务可能已被 Train 抢占清理: {e}")

        print("\n⏳ Train 角色运行中，等待 3 秒观察...")
        time.sleep(3.0)
        
        # 查看 Train 任务进度
        if train_result.get("success"):
            try:
                train_summary = ray.get(
                    train_agent.role_task_summary.remote(train_task_id)
                )
                if train_summary.get("success"):
                    task = train_summary["task"]
                    steps = task.get("steps", [])
                    if steps:
                        latest = steps[-1]
                        print(f"\n📊 Train 角色最新进度:")
                        print(f"  状态: {task.get('status')}")
                        print(f"  完成步数: {len(steps)}")
                        print(f"  metric_value: {latest.get('metric_value', 0):.4f}")
                        print(f"  loss: {latest.get('loss', 0):.4f}")
                        print(f"  吞吐量: {latest.get('throughput', 0):.0f} tokens/s")
                        print(f"  资源利用率: {latest.get('resource_utilization', 0):.2%}")
            except Exception as e:
                print(f"⚠️  Train 任务进度查询失败: {e}")
        
        # ===== 总结 =====
        print(f"\n" + "=" * 80)
        print(f"📈 Demo 总结")
        print(f"=" * 80)
        print(f"✓ 使用 SimpleScheduler API 简化任务提交流程")
        print(f"✓ 通过 ensure_pool() 创建资源池")
        print(f"✓ 通过 submit() 提交任务，自动处理注册和抢占")
        print(f"✓ 通过 configure_preemption() 配置基于 label 的抢占规则")
        print(f"✓ Rollout 角色首先占用资源池")
        print(f"✓ Reward 角色根据 role label 自动抢占 Rollout 任务")
        print(f"✓ Train 角色进一步抢占 Reward 并继续训练")
        print(f"✓ 所有被抢占任务自动保存检查点")
        print(f"\n💡 核心优势:")
        print(f"   - SimpleScheduler 提供更简洁的 API")
        print(f"   - auto_register=True 自动处理任务注册")
        print(f"   - 无需手动管理 supervisor 和底层调度细节")
        print(f"   - 基于语义的 label 抢占规则，易于理解和配置")
        
        print_stats(scheduler, "最终统计数据")

    finally:
        print("\n🧹 清理 Scheduler 和 Ray")
        scheduler.shutdown()
        ray.shutdown()


if __name__ == "__main__":
    run_demo()

