#!/usr/bin/env python3
"""
PG 池 RLHF 角色调度完整演示
===========================

场景说明
--------
我们模拟一个单池多角色的 RLHF 训练环境，角色包括：

- Rollout：生成样本，低优先级，使用动态 PG；
- Reward：评估样本，中优先级，使用预留 PG；
- Train：训练模型，高优先级，使用预留 PG。

示例演示以下能力：

1. 通过 PG 池预留 Train／Reward 的资源，Rollout 使用动态 PG；
2. 当 PG 池资源不足时，回退到传统抢占 API 释放低优任务并重新创建高优任务；
3. 扩容 Rollout 触发自动抢占（`total_preemptions > 0`），观察 PG 的复用与销毁；
4. 输出 PG 池与抢占统计，验证抢占记录；
5. 演示任务完成后的清理流程。

运行方式::

    python examples/pg_pool_rlhf_full_demo.py

运行时使用 ``local_mode=True``，便于在本地快速测试。
"""

from __future__ import annotations

import os
# Disable uv runtime-env hook to avoid psutil permission errors on restricted macOS environments.
os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")
import time
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import ray

# 允许直接导入 schedulemesh 源码
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from schedulemesh.simple import SimpleScheduler, SimpleTaskSpec
from schedulemesh.config.policy import PreemptionAggressiveness


@dataclass
class RoleSpec:
    role: str
    priority: float
    cpu: float
    memory: float
    estimated_duration: float
    count: int


BASELINE_CLUSTER_CPU = 7.0  # 默认示例需要的总 CPU 量
MIN_SCALE_FACTOR = 0.25     # 最多缩到 25%，避免出现 0 资源
scale: Optional[float] = None  # 按需初始化的全局缩放因子


def _compute_scale_factor() -> float:
    """根据 Ray 集群总 CPU 自动缩放示例资源需求."""
    try:
        cluster_resources = ray.cluster_resources()
    except Exception:
        return 1.0

    total_cpu = float(cluster_resources.get("CPU", 0.0))
    if total_cpu <= 0:
        return 1.0

    factor = total_cpu / BASELINE_CLUSTER_CPU if BASELINE_CLUSTER_CPU else 1.0
    return max(MIN_SCALE_FACTOR, min(factor, 1.0))


def scale_resource(cpu: float, memory: float) -> tuple[float, float]:
    """
    按集群规模缩放 CPU / 内存需求，并缓存缩放因子供其他场景使用.

    Returns:
        (scaled_cpu, scaled_memory)
    """
    global scale
    if scale is None:
        scale = _compute_scale_factor()

    scaled_cpu = max(round(cpu * scale, 2), 0.25)
    scaled_memory = max(round(memory * scale, 1), 256.0)
    return scaled_cpu, scaled_memory


@ray.remote
class RLHFWorker:
    def __init__(self, name: str, labels: Dict[str, str]):
        self.name = name
        self.labels = labels
        print(f"[Worker] {name} 启动，labels={labels}")

    def run(self, task_id: str, steps: int = 10, step_duration: float = 0.1):
        total = 0.0
        for idx in range(steps):
            time.sleep(step_duration)
            total += step_duration
            if idx % max(1, steps // 3) == 0:
                print(f"[Worker:{self.name}] task={task_id} progress={idx}/{steps}")
        return {"task_id": task_id, "duration": total}

    def cancel(self, task_id: str):
        print(f"[Worker:{self.name}] 任务 {task_id} 被抢占取消")
        return {"cancelled": task_id}


def print_pg_stats(scheduler: SimpleScheduler, pool: str, stage: str) -> None:
    stats = scheduler.pg_pool_stats(pool)
    print(
        f"\n[{stage}] PG 池统计: total={stats.get('total_pgs', 0)}, "
        f"high_priority={stats.get('high_priority_pgs', 0)}, "
        f"available={stats.get('available_pgs', 0)}, "
        f"allocated={stats.get('allocated_pgs', 0)}, "
        f"reuse_count={stats.get('total_reuse_count', 0)}"
    )


def print_preemption_stats(scheduler: SimpleScheduler, stage: str) -> None:
    stats = scheduler.stats()
    print(
        f"[{stage}] 抢占统计: total_preemptions={stats.get('total_preemptions', 0)}, "
        f"running_tasks={stats.get('running_tasks', 0)}"
    )


def submit_role_tasks(
    scheduler: SimpleScheduler,
    pool: str,
    spec: RoleSpec,
    prefix: str,
    *,
    fallback_to_legacy: bool = False,
) -> list[str]:
    task_ids: list[str] = []
    for idx in range(spec.count):
        task_id = f"{prefix}-{idx:02d}"
        result = scheduler.submit_with_pg_preemption(
            task_id=task_id,
            pool=pool,
            actor_class=RLHFWorker,
            resources={"cpu": spec.cpu, "memory": spec.memory},
            priority=spec.priority,
            labels={"role": spec.role, "tier": prefix, "run": "rlhf-demo"},
            estimated_duration=spec.estimated_duration,
            ray_options={"name": f"{prefix}-worker-{idx:02d}"},
        )
        success = result.get("success", False)
        if not success and fallback_to_legacy:
            print(
                f"  ⚠️  {task_id} PG 提交失败: {result.get('error', 'unknown')}. "
                "改用传统抢占路径。"
            )
            legacy_spec = SimpleTaskSpec(
                task_id=task_id,
                pool=pool,
                actor_class=RLHFWorker,
                resources={"cpu": spec.cpu, "memory": spec.memory},
                priority=spec.priority,
                labels={"role": spec.role, "tier": prefix, "run": "rlhf-demo"},
                estimated_duration=spec.estimated_duration,
                auto_register=True,
                ray_options={"name": f"{prefix}-worker-{idx:02d}"},
            )
            result = scheduler.submit_spec(legacy_spec)
            success = result.get("success", False)
            print(f"  ▶️  {task_id} 传统抢占提交: success={success}")
        else:
            print(f"  提交 {task_id}: success={success}")
        task_ids.append(task_id)
    return task_ids


def main() -> None:
    # 启动 Ray（如已有集群则连接）
    if ray.is_initialized():
        ray.shutdown()
    init_attempts: list[tuple[str, dict]] = []

    ray_address = os.environ.get("RAY_ADDRESS")
    if ray_address:
        init_attempts.append(
            (f"[Ray] 使用环境变量 RAY_ADDRESS={ray_address} 连接集群", {"address": ray_address})
        )

    init_attempts.append(
        ("[Ray] 尝试自动发现集群 (address='auto')", {"address": "auto"})
    )

    for message, kwargs in init_attempts:
        try:
            print(message)
            ray.init(ignore_reinit_error=True, **kwargs)
            break
        except Exception as exc:
            print(f"{message} 失败: {exc}")
    else:
        # 所有集群连接尝试失败，退回本地模式
        print("[Ray] 未发现可用集群，回退到本地模式 (local_mode=True)")
        ray.init(address="local", local_mode=True, ignore_reinit_error=True, num_cpus=8)

    scheduler = SimpleScheduler("pg-rlhf-full-demo")
    pool_name = "rlhf-pg-pool"

    # 配置抢占策略：Train > Reward > Rollout
    scheduler.configure_preemption(
        enable_label_preemption=True,
        label_preemption_rules={
            "role": {
                "train": ["reward", "rollout"],
                "reward": ["rollout"],
            }
        },
        preemption_aggressiveness=PreemptionAggressiveness.MEDIUM,
        enable_cross_pool_preemption=False,
    )

    # 创建启用 PG 的资源池：预留 Train/Reward，Rollout 使用动态 PG
    scheduler.ensure_pool(
        name=pool_name,
        labels={"workload": "rlhf"},
        resources={"cpu": 6.0, "memory": 8192.0},  # 池总配额
        pg_pool_config={
            "enable": True,
            "high_priority_pg_specs": [
                {"cpu": 2.0, "memory": 2048.0},  # Train 预留 PG
                {"cpu": 2.0, "memory": 2048.0},  # Reward 预留 PG
            ],
            "enable_dynamic_pgs": True,
            "max_dynamic_pgs": 2,  # Rollout 最多两个动态 PG
            "enable_pg_reuse": True,
        },
    )

    print("\n=== 场景 1: Rollout 角色启动（动态 PG） ===")
    rollout_spec = RoleSpec(role="rollout", priority=3.0, cpu=1.5, memory=1024.0, estimated_duration=45.0, count=2)
    rollout_tasks = submit_role_tasks(scheduler, pool_name, rollout_spec, "rollout")
    print_pg_stats(scheduler, pool_name, "Rollout 启动后")
    print_preemption_stats(scheduler, "Rollout 启动后")

    time.sleep(1.0)

    print("\n=== 场景 2: Reward 角色到达（高优运行） ===")
    # 优先使用预留 PG：Simple PG 池逻辑约定 priority >= 8.0 触发高优 PG
    reward_spec = RoleSpec(role="reward", priority=8.5, cpu=2.0, memory=2048.0, estimated_duration=30.0, count=1)
    reward_tasks = submit_role_tasks(
        scheduler, pool_name, reward_spec, "reward", fallback_to_legacy=True
    )
    print_pg_stats(scheduler, pool_name, "Reward 启动后")
    print_preemption_stats(scheduler, "Reward 启动后")

    time.sleep(1.0)

    print("\n=== 场景 3: Train 角色到达（预留 PG） ===")
    train_spec = RoleSpec(role="train", priority=9.0, cpu=2.0, memory=2048.0, estimated_duration=25.0, count=1)
    train_tasks: list[str] = []
    # 先尝试 PG 提交；如果池配额不足，会走回退逻辑
    pg_train_result = scheduler.submit_with_pg_preemption(
        task_id="train-00",
        pool=pool_name,
        actor_class=RLHFWorker,
        resources={"cpu": train_spec.cpu, "memory": train_spec.memory},
        priority=train_spec.priority,
        labels={"role": train_spec.role, "tier": "train", "run": "rlhf-demo"},
        estimated_duration=train_spec.estimated_duration,
        ray_options={"name": "train-worker-00"},
    )
    print(f"  提交 train-00 (PG 优先): success={pg_train_result.get('success', False)}")
    if pg_train_result.get("success"):
        train_tasks.append("train-00")
        train_handle = pg_train_result["agent"]["handle"]
        print("  ▶️ 触发 train-00 worker.run()（PG 成功分配）")
        ray.get(train_handle.run.remote("train-00", steps=20, step_duration=0.05))
    else:
        print("  ⚠️ PG 路径失败，改用传统抢占 API 触发自动抢占。")
        # 传统抢占会先抢占低优任务，再重新创建高优任务
        train_spec_legacy = SimpleTaskSpec(
            task_id="train-00",
            pool=pool_name,
            actor_class=RLHFWorker,
            resources={"cpu": train_spec.cpu, "memory": train_spec.memory},
            priority=train_spec.priority,
            labels={"role": train_spec.role, "tier": "train", "run": "rlhf-demo"},
            estimated_duration=train_spec.estimated_duration,
            auto_register=True,
            ray_options={"name": "train-worker-00"},
        )
        pg_train_result = scheduler.submit_spec(train_spec_legacy)
        print(f"  传统抢占提交 train-00: success={pg_train_result.get('success', False)}")
        if pg_train_result.get("success"):
            train_tasks.append("train-00")
            fallback_handle = pg_train_result["agent"]["handle"]
            print("  ▶️ 触发 train-00 worker.run()（传统抢占成功）")
            ray.get(fallback_handle.run.remote("train-00", steps=20, step_duration=0.05))

    print_pg_stats(scheduler, pool_name, "Train 提交后")
    print_preemption_stats(scheduler, "Train 提交后")

    print("\n=== 场景 4: Rollout 扩容触发抢占 ===")
    extra_cpu, extra_mem = scale_resource(1.5, 1024.0)
    extra_rollout = RoleSpec(role="rollout", priority=4.5, cpu=extra_cpu, memory=extra_mem, estimated_duration=20.0 * scale, count=1)
    extra_tasks: list[str] = []
    # 直接使用传统抢占 API，观察额外 Rollout 触发自动抢占的过程
    extra_spec = SimpleTaskSpec(
        task_id="rollout-extra-00",
        pool=pool_name,
        actor_class=RLHFWorker,
        resources={"cpu": extra_rollout.cpu, "memory": extra_rollout.memory},
        priority=extra_rollout.priority,
        labels={"role": extra_rollout.role, "tier": "rollout-extra", "run": "rlhf-demo"},
        estimated_duration=extra_rollout.estimated_duration,
        auto_register=True,
        ray_options={"name": "rollout-extra-worker-00"},
    )
    extra_result = scheduler.submit_spec(extra_spec)
    print(f"  提交 rollout-extra-00 (传统抢占): success={extra_result.get('success', False)}")
    if extra_result.get("success"):
        extra_tasks.append("rollout-extra-00")
        rollout_extra_handle = extra_result["agent"]["handle"]
        print("  ▶️ 触发 rollout-extra-00 worker.run()")
        ray.get(rollout_extra_handle.run.remote("rollout-extra-00", steps=15, step_duration=0.05))

    print_pg_stats(scheduler, pool_name, "扩容后")
    print_preemption_stats(scheduler, "扩容后")

    print("\n=== 场景 5: 汇总统计 ===")
    print_pg_stats(scheduler, pool_name, "最终")
    print_preemption_stats(scheduler, "最终")
    print("\n📈 指标观察: curl 127.0.0.1:8080/metrics | rg schedulemesh_ 可查看实时抢占/调度指标")
    print("   关注 schedulemesh_preemption_count, schedulemesh_preemption_execution_latency_ms 等指标了解 PG 效果。")

    print("\n=== 场景 6: 清理任务 ===")
    all_tasks = rollout_tasks + reward_tasks + train_tasks + extra_tasks
    random.shuffle(all_tasks)
    for task_id in all_tasks:
        scheduler.complete(task_id)
        deletion = scheduler.scheduler.delete_agent(task_id, force=True, destroy_pg=True)
        print(f"  清理 {task_id}: destroy_pg=True, success={deletion.get('success', False)}")
    scheduler.shutdown()
    ray.shutdown()
    print("清理完成。")


if __name__ == "__main__":
    main()
