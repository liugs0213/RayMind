"""
RLHF 角色调度抢占 Demo（CPU 本地模拟）。

演示真实的 RLHF 训练场景中的角色协同与抢占：
1. Rollout 角色启动，负责生成样本数据
2. Reward 角色到达，需要评估样本质量，可以抢占 Rollout
3. Train 角色到达，需要更新模型参数，可以抢占 Reward 和 Rollout
4. 展示不同角色的优先级关系和资源分配

RLHF 角色说明：
- Train（训练器）：最高优先级，负责更新模型参数，需要大量计算资源
- Reward（奖励模型）：中等优先级，负责评估生成质量，需要中等计算资源
- Rollout（采样器）：较低优先级，负责生成样本数据，资源需求相对灵活
- Critic（评论器）：中等优先级，负责价值估计

核心特性：
- ✅ 基于 label 的角色抢占（train > reward > rollout）
- ✅ 真实的 RLHF 训练角色模拟
- ✅ 不同角色的资源需求差异
- ✅ 检查点保存与恢复机制
- ✅ 完全 CPU 本地运行，无需 GPU

运行方式：
    python examples/training_preemption_demo.py

Ray 参数：
- local_mode=True: 进程内执行，便于调试
- num_cpus=4: 限制 CPU 核心数
- ignore_reinit_error=True: 忽略重复初始化
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

from schedulemesh.config.policy import PreemptionAggressiveness

from schedulemesh.core.agents import MetricsReportingAgent
from schedulemesh.core.controllers.ray_scheduler import RayScheduler


@dataclass
class RLHFRoleConfig:
    """RLHF 角色配置"""
    role: str  # train, reward, rollout, critic
    model_size: str  # 7B, 13B, 70B
    batch_size: int
    learning_rate: float
    gpu_required: float
    cpu_required: float
    memory_required: float  # GB


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
class RLHFRoleAgentActor(MetricsReportingAgent):
    """
    模拟真实的 RLHF 角色 Agent，支持资源管理和检查点保存。

    特性：
    - 支持不同 RLHF 角色（train、reward、rollout、critic）
    - 模拟 GPU/CPU/内存资源使用
    - 支持检查点保存与恢复
    - 模拟真实的角色工作负载和资源利用率
    """

    def __init__(
        self,
        name: str,
        labels: Dict[str, str],
        supervisor: Optional[ray.actor.ActorHandle] = None,
        *,
        report_interval: float = 1.0,
    ):
        super().__init__(
            name,
            labels,
            supervisor,
            report_interval=report_interval,
            max_pending_reports=64,
        )
        
        # 从 Ray 的资源配置中获取资源信息
        try:
            # 获取当前 actor 的资源限制
            resource_limits = ray.get_resource_limits()
            self.resources = {
                "cpu": resource_limits.get("CPU", 2.0),
                "memory": resource_limits.get("memory", 4096.0),
                "gpu": resource_limits.get("GPU", 1.0),
            }
        except Exception:
            # 如果无法获取资源配置，使用默认值
            self.resources = {"cpu": 2.0, "memory": 4096.0, "gpu": 1.0}
        
        # 当前资源使用情况
        self.resource_usage = {"cpu": 0.0, "memory": 0.0, "gpu": 0.0}

        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._task_threads: Dict[str, threading.Thread] = {}
        self._stop_flags: Dict[str, threading.Event] = {}
        self._lock = threading.Lock()
        
        print(f"📦 RLHFRoleAgent '{name}' 初始化 | 资源配额: {self.resources}")

    # ---- 资源管理 ---------------------------------------------------------
    
    def get_available_resources(self) -> dict:
        """获取可用资源"""
        with self._lock:
            available = {
                res: total - self.resource_usage.get(res, 0.0)
                for res, total in self.resources.items()
            }
        return {"success": True, "available": available, "total": self.resources}
    
    def _allocate_resources(self, config: RLHFRoleConfig) -> bool:
        """尝试分配资源"""
        with self._lock:
            # 检查资源是否足够
            required = {
                "cpu": config.cpu_required,
                "memory": config.memory_required,
                "gpu": config.gpu_required,
            }
            for res, amount in required.items():
                available = self.resources.get(res, 0.0) - self.resource_usage.get(res, 0.0)
                if available < amount:
                    return False
            
            # 分配资源
            for res, amount in required.items():
                self.resource_usage[res] = self.resource_usage.get(res, 0.0) + amount
            return True
    
    def _release_resources(self, config: RLHFRoleConfig) -> None:
        """释放资源"""
        with self._lock:
            self.resource_usage["cpu"] = max(0.0, self.resource_usage.get("cpu", 0.0) - config.cpu_required)
            self.resource_usage["memory"] = max(0.0, self.resource_usage.get("memory", 0.0) - config.memory_required)
            self.resource_usage["gpu"] = max(0.0, self.resource_usage.get("gpu", 0.0) - config.gpu_required)

    # ---- RLHF 角色任务模拟 ---------------------------------------------------------

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
        # 检查并分配资源
        if not self._allocate_resources(config):
            return {
                "success": False,
                "error": "Insufficient resources",
                "required": {
                    "cpu": config.cpu_required,
                    "memory": config.memory_required,
                    "gpu": config.gpu_required,
                },
                "available": self.get_available_resources()["available"],
            }
        
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
                self._release_resources(config)
                return {"success": False, "error": f"Task '{task_id}' already running"}
            self._tasks[task_id] = summary
            self._stop_flags[task_id] = stop_flag

        def _run_role_task():
            total_metric = 0.0
            total_throughput = 0.0
            
            # 根据 RLHF 角色调整基础参数
            if config.role == "train":
                # Train: 训练模型，需要最多资源，输出loss
                base_metric = 0.5  # 初始loss
                exploration = 0.1
                base_throughput = 2000.0  # tokens/sec
                resource_util_base = 0.95
                metric_name = "loss"
            elif config.role == "reward":
                # Reward: 评估样本质量，输出reward分数
                base_metric = 0.6  # 初始reward分数
                exploration = 0.15
                base_throughput = 3000.0  # samples/sec
                resource_util_base = 0.85
                metric_name = "reward_score"
            elif config.role == "rollout":
                # Rollout: 生成样本数据，资源需求相对灵活
                base_metric = 0.7  # 初始生成质量
                exploration = 0.1
                base_throughput = 5000.0  # samples/sec
                resource_util_base = 0.75
                metric_name = "generation_quality"
            elif config.role == "critic":
                # Critic: 价值估计
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
                
                self.report_metrics(
                    {
                        "role": config.role,
                        "current_step": step_idx,
                        "loss": loss,
                        metric_name: metric_value,
                        "throughput": throughput,
                        "resource_utilization": resource_util,
                    }
                )
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
            
            final_metrics = {
                "role": config.role,
                "current_step": len(role_steps),
                "status": summary.get("status", "running"),
                "total_metric": summary["total_metric"],
                "avg_throughput": summary["avg_throughput"],
            }
            if role_steps:
                final_metrics["loss"] = role_steps[-1].loss
                final_metrics[metric_name] = role_steps[-1].metric_value
                final_metrics["throughput"] = role_steps[-1].throughput
            self.report_metrics(final_metrics, force=True)
            
            # 释放资源
            self._release_resources(config)
            self.flush_metrics()

        worker = threading.Thread(target=_run_role_task, name=f"role-{task_id}", daemon=True)
        with self._lock:
            self._task_threads[task_id] = worker
        worker.start()
        
        print(
            f"🚀 启动 RLHF 角色任务 '{task_id}' | "
            f"角色={config.role} 模型={config.model_size} "
            f"资源=[GPU:{config.gpu_required}, CPU:{config.cpu_required}, "
            f"MEM:{config.memory_required}GB]"
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


def _print_header(title: str) -> None:
    print("\n" + title)
    print("-" * len(title))


def run_demo() -> None:
    print("=" * 80)
    print("🚀 RLHF 角色调度抢占 Demo（CPU 本地模拟）")
    print("=" * 80)

    ray_config = {
        "ignore_reinit_error": True,
        "local_mode": True,  # 进程内执行，便于调试
        "num_cpus": 4,      # 限制 CPU 核心数
        "include_dashboard": False,  # 关闭 dashboard
        "log_to_driver": False,     # 减少日志输出
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
    scheduler = RayScheduler("rlhf-role-preemption-demo")

    def print_preemption_stats(title: str) -> None:
        stats = scheduler.get_preemption_stats()
        print(f"📊 {title}")
        print(f"  当前运行任务: {stats.get('running_tasks', 0)}")
        print(f"  累计抢占次数: {stats.get('total_preemptions', 0)}")
        recent = stats.get("recent_preemptions") or []
        if recent:
            print("  最近抢占记录:")
            for record in recent:
                task_id = record.get("task_id")
                pool = record.get("pool_name")
                reason = record.get("reason")
                cancel_success = record.get("cancel_success")
                print(
                    f"    - task={task_id} pool={pool} reason={reason} "
                    f"cancel_success={cancel_success}"
                )
        else:
            print("  最近抢占记录: 无")

    try:
        print("📋 配置抢占策略...")
        scheduler.update_preemption_policy(
            enable_label_preemption=True,
            label_preemption_rules={
                "role": {
                    "train": ["reward", "critic", "rollout"],
                    "reward": ["rollout"],
                    "critic": ["rollout"],
                }
            },
            preemption_aggressiveness=PreemptionAggressiveness.MEDIUM,
            cross_pool_priority_threshold=5.0,
            enable_cross_pool_preemption=True,
        )
        print("✅ 抢占策略: Train > Reward/Critic > Rollout")

        pool_name = "rlhf-shared-pool"
        print(f"🏗️  创建资源池 {pool_name} ...")
        scheduler.create_pool(
            name=pool_name,
            labels={"workload": "rlhf", "tier": "shared"},
            resources={"cpu": 2.0, "memory": 4096.0, "gpu": 0.0},
            target_agents=1,
        )
        print(f"  ✅ {pool_name}: 0x GPU, 2 CPU, 4GB Memory")

        active_agents: Dict[str, Dict[str, Any]] = {}

        def launch_role_task(
            *,
            task_id: str,
            config: RLHFRoleConfig,
            steps: int,
            step_duration: float,
            checkpoint_interval: int,
        ) -> Optional[Dict[str, Any]]:
            resources = {
                "cpu": config.cpu_required,
                "memory": config.memory_required,
                "gpu": config.gpu_required,
            }
            mem_gb = resources["memory"] / 1024.0 if resources["memory"] else 0.0
            print(f"{'=' * 80}")
            print(f"🎬 启动角色任务 {config.role.upper()} -> {task_id}")
            print(f"{'=' * 80}")
            print(f"模型规模: {config.model_size}")
            print(f"批大小: {config.batch_size}")
            print(f"资源需求: {resources['cpu']} CPU, {mem_gb:.1f} GB Memory")

            submission = scheduler.submit_task_with_preemption(
                task_id=task_id,
                pool_name=pool_name,
                resources=resources,
                priority=5.0,
                labels={
                    "job": "training",
                    "role": config.role,
                    "model_size": config.model_size,
                },
                actor_class=RLHFRoleAgentActor,
                actor_kwargs={
                    "supervisor": scheduler.supervisor_handle(),
                    "report_interval": 1.0,
                },
            )

            if not submission.get("success"):
                print(
                    f"✗ 任务提交失败: {submission.get('error')} "
                    f"(reason={submission.get('reason')})"
                )
                return None

            agent_info = submission["agent"]
            active_agents[task_id] = agent_info
            scheduler.register_running_task(
                task_id=task_id,
                agent_name=agent_info["name"],
                pool_name=pool_name,
                priority=5.0,
                labels={
                    "job": "training",
                    "role": config.role,
                    "model_size": config.model_size,
                },
                estimated_duration=1200.0,
            )

            handle = agent_info["handle"]
            start_result = ray.get(
                handle.start_role_task.remote(
                    task_id=task_id,
                    config=config,
                    steps=steps,
                    step_duration=step_duration,
                    checkpoint_interval=checkpoint_interval,
                )
            )
            if start_result.get("success"):
                print(f"✓ 任务已启动 (agent={agent_info['name']})")
            else:
                print(f"✗ 任务启动失败: {start_result.get('error')}")
            return agent_info

        rollout_task_id = "rollout-data-collection"
        rollout_config = RLHFRoleConfig(
            role="rollout",
            model_size="13B",
            batch_size=24,
            learning_rate=5e-5,
            gpu_required=0.0,
            cpu_required=2.0,
            memory_required=2048.0,
        )
        rollout_agent = launch_role_task(
            task_id=rollout_task_id,
            config=rollout_config,
            steps=90,
            step_duration=0.12,
            checkpoint_interval=15,
        )

        if rollout_agent:
            print("⏳ Rollout 角色运行中，等待 3 秒观察...")
            time.sleep(3.0)

        reward_task_id = "reward-model-eval"
        reward_config = RLHFRoleConfig(
            role="reward",
            model_size="13B",
            batch_size=16,
            learning_rate=1e-4,
            gpu_required=0.0,
            cpu_required=2.0,
            memory_required=2304.0,
        )
        reward_agent = launch_role_task(
            task_id=reward_task_id,
            config=reward_config,
            steps=80,
            step_duration=0.14,
            checkpoint_interval=12,
        )

        print_preemption_stats("Reward 角色提交后的抢占统计")

        if reward_agent:
            print("⏳ Reward 角色运行中，等待 2 秒观察...")
            time.sleep(2.0)

        if rollout_agent:
            try:
                ray.get(
                    rollout_agent["handle"].role_task_summary.remote(rollout_task_id)
                )
            except Exception:
                print("ℹ️  Rollout 任务句柄已清理，说明抢占成功。")

        train_task_id = "rlhf-train-step"
        train_config = RLHFRoleConfig(
            role="train",
            model_size="7B",
            batch_size=32,
            learning_rate=2e-5,
            gpu_required=0.0,
            cpu_required=2.0,
            memory_required=3072.0,
        )
        train_agent = launch_role_task(
            task_id=train_task_id,
            config=train_config,
            steps=70,
            step_duration=0.16,
            checkpoint_interval=10,
        )

        print_preemption_stats("Train 角色提交后的抢占统计")

        if reward_agent:
            try:
                ray.get(
                    reward_agent["handle"].role_task_summary.remote(reward_task_id)
                )
            except Exception:
                print("ℹ️  Reward 任务句柄已清理，说明 Train 成功抢占。")

        if train_agent:
            print("⏳ Train 角色运行中，等待 3 秒观察...")
            time.sleep(3.0)
            try:
                train_summary = ray.get(
                    train_agent["handle"].role_task_summary.remote(train_task_id)
                )
            except Exception as exc:
                print(f"⚠️  无法获取 Train 任务进度: {exc}")
            else:
                if train_summary.get("success"):
                    task = train_summary["task"]
                    steps = task.get("steps", [])
                    if steps:
                        latest = steps[-1]
                        metric_label = task.get("metric_name", "metric_value")
                        print("📊 Train 角色最新进度:")
                        print(f"  状态: {task.get('status')}")
                        print(f"  完成 step: {len(steps)}")
                        print(f"  {metric_label}: {latest.get('metric_value', 0):.4f}")
                        print(f"  loss: {latest.get('loss', 0):.4f}")
                        print(f"  吞吐量: {latest.get('throughput', 0):.0f} tokens/s")
                else:
                    print(f"⚠️  Train 任务进度查询失败: {train_summary.get('error')}")

        print(f"" + "=" * 80)
        print(f"📈 Demo 总结")
        print(f"=" * 80)
        print(f"✓ Rollout 角色首先占用资源池")
        print(f"✓ Reward 角色根据 role label 抢占 Rollout 任务")
        print(f"✓ Train 角色进一步抢占 Reward 并继续训练")
        print(f"✓ get_preemption_stats() 展示抢占历史与统计数据")
        print(f"✓ submit_task_with_preemption 自动串联资源预留、抢占与 Agent 创建")

    finally:
        print("清理 Ray / Scheduler")
        scheduler.shutdown()
        ray.shutdown()


if __name__ == "__main__":
    run_demo()
