#!/usr/bin/env python3
"""
自动化抢占任务提交示例

演示如何使用 SimpleScheduler 提供的高级 API 来自动处理资源不足的情况。
当资源不足时，系统会自动评估并执行抢占操作。
"""

import logging
import sys
import time
from pathlib import Path

import ray

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from schedulemesh.simple import SimpleScheduler
from schedulemesh.config.policy import PreemptionAggressiveness
from schedulemesh.simple.utils import (
    configure_demo_logging,
    demote_ray_logging,
    describe_submission,
    suppress_actor_prefix,
)


@ray.remote
class SimpleTaskActor:
    """简单的任务Actor，用于演示"""
    
    def __init__(self, name: str, labels: dict[str, str], *, duration: float = 10.0):
        self.name = name
        self.labels = labels
        self.duration = duration
        self.start_time = time.time()
        print(f"任务 {name} 开始执行，预计耗时 {duration} 秒")
    
    def run(self):
        """执行任务"""
        time.sleep(self.duration)
        elapsed = time.time() - self.start_time
        print(f"任务 {self.name} 完成，实际耗时 {elapsed:.2f} 秒")
        return {"task_id": self.name, "duration": elapsed}
    
    def cancel(self, task_id: str):
        """取消任务"""
        print(f"任务 {task_id} 被抢占取消")
        return {"cancelled": True, "task_id": task_id}


def main():
    """主函数：演示自动化抢占功能"""
    
    logger = configure_demo_logging()
    demote_ray_logging()
    suppress_actor_prefix()

    # 智能初始化 Ray
    if not ray.is_initialized():
        try:
            # 尝试连接现有集群
            ray.init(address="auto", ignore_reinit_error=True)
            print("✅ 连接到现有 Ray 集群")
        except Exception:
            # 创建新本地集群
            ray.init(local_mode=True)
            print("📋 创建新的本地 Ray 集群 (local_mode)")
    
    # 创建简化调度器
    scheduler = SimpleScheduler("preemption-demo")
    
    print("=== 自动化抢占任务提交演示 ===\n")
    
    def pretty_print_stats(stats: dict, title: str) -> None:
        print(title)
        print(f"  - 总抢占次数: {stats.get('total_preemptions', 0)}")
        print(f"  - 同池抢占: {stats.get('same_pool_preemptions', 0)}")
        print(f"  - 跨池抢占: {stats.get('cross_pool_preemptions', 0)}")
        print(f"  - 当前登记运行任务: {stats.get('running_tasks', 0)}")
        recent = stats.get("recent_preemptions") or []
        if recent:
            print("  - 最近抢占记录:")
            for record in recent:
                cancel = "成功" if record.get("cancel_success") else "失败"
                print(
                    f"    • 任务 {record.get('task_id')}@{record.get('pool_name')} "
                    f"原因={record.get('reason')} 取消={cancel}"
                )
        else:
            print("  - 最近抢占记录: 无")
        print()

    # 1. 创建资源池
    print("1. 创建资源池...")
    pool_result = scheduler.ensure_pool(
        name="demo-pool",
        labels={"tier": "standard"},
        resources={"cpu": 1, "memory": 2.0},
        target_agents=1
    )
    if pool_result.get("success"):
        pool_snapshot = pool_result.get("pool", {})
        logging.getLogger("SimpleDemo").info(
            "资源池创建成功 名称=%s 标签=%s 容量=%s 默认Agent=%s",
            pool_snapshot.get("name"),
            pool_snapshot.get("labels"),
            pool_snapshot.get("capacity"),
            pool_snapshot.get("default_agent_resources"),
        )
    else:
        logging.getLogger("SimpleDemo").error("资源池创建失败: %s", pool_result)
    
    # 2. 配置抢占策略
    print("2. 配置抢占策略...")
    policy_result = scheduler.configure_preemption(
        preemption_aggressiveness=PreemptionAggressiveness.MEDIUM,
        enable_label_preemption=True,
        label_preemption_rules={
            "tier": {
                "premium": ["standard", "batch"],
                "standard": ["batch"]
            }
        }
    )
    if policy_result.get("success"):
        policy = policy_result.get("policy", {})
        logging.getLogger("SimpleDemo").info(
            "抢占策略配置成功 同池阈值=%s 跨池阈值=%s 标签阈值=%s 规则=%s",
            policy.get("same_pool_priority_threshold"),
            policy.get("cross_pool_priority_threshold"),
            policy.get("label_priority_threshold"),
            policy.get("label_preemption_rules"),
        )
    else:
        logging.getLogger("SimpleDemo").error("抢占策略配置失败: %s", policy_result)
    
    # 3. 提交第一个任务（低优先级）
    logging.getLogger("SimpleDemo").info("提交第一个任务（低优）")
    task1_result = scheduler.submit(
        task_id="task-1",
        pool="demo-pool",
        resources={"cpu": 1, "memory": 2.0},
        priority=3.0,  # 低优先级
        labels={"tier": "standard", "user": "alice"},
        actor_class=SimpleTaskActor,
        actor_kwargs={"duration": 30.0}
    )
    describe_submission(task1_result, "任务1提交结果")
    if task1_result["success"]:
        agent1 = task1_result["agent"]["handle"]
        # 启动第一个任务（后台执行）
        first_run = agent1.run.remote()
        logging.getLogger("SimpleDemo").info("任务1已启动，等待 2 秒以模拟运行中状态")
    else:
        first_run = None
    
    # 等待一下让第一个任务开始运行
    time.sleep(2)
    
    # 4. 提交第二个任务（高优先级，会触发抢占）
    logging.getLogger("SimpleDemo").info("提交第二个任务（高优，触发抢占）")
    task2_result = scheduler.submit(
        task_id="task-2",
        pool="demo-pool",
        resources={"cpu": 1, "memory": 2.0},
        priority=8.0,  # 高优先级
        labels={"tier": "premium", "user": "bob"},
        actor_class=SimpleTaskActor,
        actor_kwargs={"duration": 6.0}
    )
    describe_submission(task2_result, "任务2提交结果")
    if task2_result["success"]:
        agent2 = task2_result["agent"]["handle"]
        second_run = agent2.run.remote()
    else:
        agent2 = None
        second_run = None
    
    # 5. 等待任务完成
    print("5. 等待任务完成...")
    time.sleep(5)
    if second_run:
        logging.getLogger("SimpleDemo").info("等待高优先级任务收尾")
        ray.get(second_run)
        print("任务2已完成。\n")
    
    # 6. 查看抢占统计
    stats = scheduler.stats()
    pretty_print_stats(stats, "自动抢占阶段统计")
    if task1_result["success"]:
        scheduler.complete("task-1")
    if task2_result["success"]:
        scheduler.complete("task-2")

    # ========== 手动指定跨 Pool 抢占演示 ==========
    logging.getLogger("SimpleDemo").info("=== 手动跨 Pool 抢占演示 ===")

    standard_pool = "standard-pool"
    premium_pool = "premium-pool"

    scheduler.ensure_pool(
        name=standard_pool,
        labels={"tier": "standard"},
        resources={"cpu": 1, "memory": 2.0},
        target_agents=1,
    )
    scheduler.ensure_pool(
        name=premium_pool,
        labels={"tier": "premium"},
        resources={"cpu": 1, "memory": 2.0},
        target_agents=1,
    )

    logging.getLogger("SimpleDemo").info("提交标准池低优任务 task-std")
    std_result = scheduler.submit(
        task_id="task-std",
        pool=standard_pool,
        resources={"cpu": 1, "memory": 2.0},
        priority=2.0,
        labels={"tier": "standard"},
        actor_class=SimpleTaskActor,
        actor_kwargs={"duration": 25.0},
    )
    describe_submission(std_result, "task-std 提交结果")

    std_agent_name = None
    if std_result["success"]:
        std_agent_name = std_result["agent"]["name"]
        std_run = std_result["agent"]["handle"].run.remote()
        logging.getLogger("SimpleDemo").info("task-std 正在运行")
        time.sleep(2)
    else:
        std_run = None

    logging.getLogger("SimpleDemo").info("手动发起 premium 池高优任务 (task-prem)，先评估候选")
    manual_eval = scheduler.scheduler.evaluate_preemption(
        incoming_task_priority=9.0,
        incoming_task_pool=premium_pool,
        incoming_task_labels={"tier": "premium"},
        # 不传递 incoming_task_resources，让系统自动从Pool获取默认资源
    )
    def describe_candidates(title: str, evaluation: dict) -> None:
        print(title)
        if not evaluation.get("should_preempt"):
            print("  - 无可抢占对象\n")
            return
        candidates = evaluation.get("candidates") or []
        print(f"  - 候选数量: {len(candidates)}")
        for idx, candidate in enumerate(candidates, start=1):
            print(
                f"    • #{idx} 任务={candidate.get('task_id')} "
                f"Agent={candidate.get('agent_name')} Pool={candidate.get('pool_name')} "
                f"得分={candidate.get('preempt_score'):.2f} 原因={candidate.get('reason')}"
            )
        print()

    describe_candidates("评估结果:", manual_eval)

    if manual_eval.get("should_preempt") and std_agent_name:
        logging.getLogger("SimpleDemo").info("指定 victim agent，执行手动抢占")
        manual_preempt = scheduler.scheduler.preempt_task(
            incoming_task_priority=9.0,
            incoming_task_pool=premium_pool,
            incoming_task_labels={"tier": "premium"},
            target_agent_name=std_agent_name,
        )
        if manual_preempt.get("success"):
            logging.getLogger("SimpleDemo").info("手动抢占成功")
            describe_candidates("  - 抢占评估回放", manual_preempt.get("evaluation", {}))
        else:
            logging.getLogger("SimpleDemo").error("手动抢占失败: %s", manual_preempt.get("reason"))

        scheduler.scheduler.delete_agent(std_agent_name, force=True)
        if std_run:
            try:
                ray.get(std_run)
            except Exception:
                pass
        scheduler.complete("task-std")
    else:
        logging.getLogger("SimpleDemo").warning("没有找到可抢占对象，跳过手动抢占演示")
        std_run = None

    logging.getLogger("SimpleDemo").info("抢占后提交 premium 任务 task-prem")
    prem_result = scheduler.submit(
        task_id="task-prem",
        pool=premium_pool,
        resources={"cpu": 1, "memory": 2.0},
        priority=9.0,
        labels={"tier": "premium"},
        actor_class=SimpleTaskActor,
        actor_kwargs={"duration": 8.0},
    )
    describe_submission(prem_result, "task-prem 提交结果")

    if prem_result["success"]:
        prem_run = prem_result["agent"]["handle"].run.remote()
        ray.get(prem_run)

    final_stats = scheduler.stats()
    pretty_print_stats(final_stats, "手动跨池抢占阶段统计")
    if prem_result["success"]:
        scheduler.complete("task-prem")
    
    # 7. 清理
    logging.getLogger("SimpleDemo").info("清理资源")
    scheduler.shutdown()
    ray.shutdown()
    logging.getLogger("SimpleDemo").info("演示完成")


if __name__ == "__main__":
    main()

