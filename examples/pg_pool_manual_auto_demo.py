#!/usr/bin/env python3
"""
PG 池自动抢占 + 手动抢占演示
==============================

该示例展示了如何在同一个 PG 池中：

1. 使用 ``submit_with_pg_preemption`` 触发 **自动抢占**；
2. 使用 ``RayScheduler`` 的 API 进行 **手动抢占**。

运行方式::

    python examples/pg_pool_manual_auto_demo.py

注意：示例使用 ``local_mode=True`` 在本机调度，适合测试和调试。
"""

from __future__ import annotations

import time
from pathlib import Path
import sys

import ray

# 确保可以直接导入 schedulemesh
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from schedulemesh.simple import SimpleScheduler


@ray.remote
class DemoWorker:
    """简单示例 Actor，支持 cancel 调用。"""

    def __init__(self, name: str, labels: dict[str, str]):
        self.name = name
        self.labels = labels
        print(f"[DemoWorker] {name} 启动，labels={labels}")

    def run(self, duration: float = 2.0) -> dict:
        time.sleep(duration)
        return {"agent": self.name, "duration": duration}

    def cancel(self, task_id: str) -> dict:
        print(f"[DemoWorker] {self.name} 接到取消任务 {task_id}")
        return {"success": True, "cancelled": task_id}


def print_pg_stats(scheduler: SimpleScheduler, pool_name: str, title: str) -> None:
    pg_stats = scheduler.pg_pool_stats(pool_name)
    print(f"\n[{title}] PG 池统计: total={pg_stats.get('total_pgs', 0)}, "
          f"available={pg_stats.get('available_pgs', 0)}, "
          f"reuse_count={pg_stats.get('total_reuse_count', 0)}")


def print_preemption_stats(scheduler: SimpleScheduler, title: str) -> None:
    stats = scheduler.stats()
    print(f"\n[{title}] 抢占统计: total_preemptions={stats.get('total_preemptions', 0)}, "
          f"running_tasks={stats.get('running_tasks', 0)}")


def print_alive_agents(scheduler: SimpleScheduler, pool_name: str, title: str) -> None:
    agents_result = scheduler.scheduler.list_agents(pool=pool_name)
    agents = agents_result.get("agents", []) if isinstance(agents_result, dict) else []
    print(f"\n[{title}] 当前 ALIVE Agent:")
    if not agents:
        print("  (无存活 Agent)")
        return
    for agent in agents:
        name = agent.get("name")
        status = agent.get("status")
        labels = agent.get("labels")
        pg_info = agent.get("pg_info")
        print(f"  - {name} status={status} labels={labels} pg={pg_info}")


def main() -> None:
    # 处理已有 Ray 连接的情况，优先使用本地 local_mode
    if ray.is_initialized():
        ray.shutdown()
    try:
        ray.init(ignore_reinit_error=True, local_mode=True, num_cpus=4)
    except ValueError as exc:
        # 如果检测到已有集群，自动连接并避免传递资源限制
        if "connecting to an existing cluster" in str(exc).lower():
            print("[Ray] 检测到已有集群，切换为 address=\"auto\" 连接。")
            ray.init(address="auto", ignore_reinit_error=True)
        else:
            raise
    scheduler = SimpleScheduler("pg-manual-auto-demo")
    pool_name = "pg-manual-auto-pool"

    try:
        # 仅允许一个动态 PG，这样第二个任务就会触发抢占逻辑
        scheduler.ensure_pool(
            name=pool_name,
            resources={"cpu": 2.0, "memory": 2048.0},
            pg_pool_config={
                "enable": True,
                "high_priority_pg_specs": [],   # 不预留资源，方便看到抢占效果
                "enable_dynamic_pgs": True,
                "max_dynamic_pgs": 1,            # 只允许一个动态 PG
                "enable_pg_reuse": True,
            },
        )
        print("\n✅ PG 池初始化完成")
        print_pg_stats(scheduler, pool_name, "初始化")

        # ------------------------------------------------------------------
        # 1. 手动抢占示例
        # ------------------------------------------------------------------
        print("\n=== 场景一：手动抢占 ===")
        baseline_task_id = "manual-low-001"
        baseline_result = scheduler.submit_with_pg_preemption(
            task_id=baseline_task_id,
            pool=pool_name,
            actor_class=DemoWorker,
            resources={"cpu": 2.0, "memory": 1024.0},
            priority=3.0,
            labels={"scenario": "manual", "priority": "baseline"},
            estimated_duration=30.0,
            ray_options={"name": "ManualBaselineWorker"},
        )
        print(f"🛠️ 基线任务提交: {baseline_result.get('success', False)}")
        print_pg_stats(scheduler, pool_name, "基线任务占用后")
        print_alive_agents(scheduler, pool_name, "基线任务占用后")

        # 先尝试直接提交高优任务（期望失败，再转向手动抢占）
        manual_attempt_id = "manual-high-attempt"
        print("\n⏱️  尝试直接提交高优任务（期待失败触发手动抢占）...")
        manual_attempt = scheduler.submit_with_pg_preemption(
            task_id=manual_attempt_id,
            pool=pool_name,
            actor_class=DemoWorker,
            resources={"cpu": 2.0, "memory": 1024.0},
            priority=8.0,
            labels={"scenario": "manual", "priority": "urgent"},
            estimated_duration=30.0,
            ray_options={"name": "ManualHighAttempt"},
        )
        if manual_attempt.get("success"):
            print("⚠️ 高优任务意外自建成功，先删除后再演示手动抢占。")
            scheduler.complete(manual_attempt_id)
            scheduler.scheduler.delete_agent(manual_attempt_id, force=True)
        else:
            print(f"❌ 高优任务直接提交失败（预期，已清理回退 Agent）：{manual_attempt.get('error')}")
            print("ℹ️️  上方可能出现的 'manual-high-attempt 已删除' 日志是回退清理，不是抢占结果。")

        # 评估抢占候选
        incoming_priority = 8.0
        incoming_resources = {"cpu": 2.0, "memory": 1024.0}
        eval_result = scheduler.scheduler.evaluate_preemption(
            incoming_task_priority=incoming_priority,
            incoming_task_pool=pool_name,
            incoming_task_labels={"scenario": "manual", "priority": "urgent"},
            incoming_task_resources=incoming_resources,
        )
        print(f"\n📋 手动抢占评估结果: should_preempt={eval_result.get('should_preempt')}")
        candidates = eval_result.get("candidates") or []
        for idx, candidate in enumerate(candidates, start=1):
            print(f"  候选 {idx}: task={candidate.get('task_id')} "
                  f"agent={candidate.get('agent_name')} "
                  f"score={candidate.get('preempt_score'):.2f}")

        if not candidates:
            print("⚠️ 没有可抢占的候选，手动抢占示例结束。")
        else:
            victim = candidates[0]
            victim_task_id = victim.get("task_id")
            victim_agent_name = victim.get("agent_name")
            print(f"\n🔧 手动抢占执行，选择 {victim_task_id} / {victim_agent_name}")

            scheduler.scheduler.execute_preemption(victim_task_id, victim_agent_name)
            # 删除被抢占的 agent 时保留其 PG，展示复用效果
            supervisor = scheduler.scheduler.supervisor_handle()
            ray.get(supervisor.delete_agent.remote(victim_agent_name, force=True, destroy_pg=False))
            scheduler.complete(victim_task_id)
            print_pg_stats(scheduler, pool_name, "手动抢占后")
            print_preemption_stats(scheduler, "手动抢占后")
            print_alive_agents(scheduler, pool_name, "手动抢占后")

            # 抢占释放后，在相同资源需求下重新提交高优任务
            manual_high_result = scheduler.submit_with_pg_preemption(
                task_id="manual-high-001",
                pool=pool_name,
                actor_class=DemoWorker,
                resources=incoming_resources,
                priority=incoming_priority,
                labels={"scenario": "manual", "priority": "urgent"},
                estimated_duration=15.0,
                ray_options={"name": "ManualHighWorker"},
            )
            print(f"\n🚀 手动抢占释放资源后，高优任务提交成功: {manual_high_result.get('success', False)}")
            print_pg_stats(scheduler, pool_name, "手动抢占 + 重新提交后")
            print_alive_agents(scheduler, pool_name, "手动抢占 + 重新提交后")

    finally:
        print("\n🧹 清理资源")
        scheduler.shutdown()
        ray.shutdown()


if __name__ == "__main__":
    main()
