#!/usr/bin/env python3
"""
优先级调度演示

展示：
1. 高优先级任务优先调度
2. 相同优先级按 FIFO
3. 不同 label 队列独立
4. 与抢占功能的配合
"""

import ray

from schedulemesh.core import RayScheduler
from schedulemesh.core.agent_actor import AgentActor


def demo_basic_priority_scheduling():
    """演示基本优先级调度"""
    print("\n" + "=" * 60)
    print("示例 1: 基本优先级调度")
    print("=" * 60)
    
    scheduler = RayScheduler("priority-demo")
    
    # 创建资源池和 Agent
    print("\n1. 创建资源池和 Agent")
    scheduler.create_pool(
        name="compute-pool",
        labels={"tier": "standard"},
        resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_agent(
        name="worker-1",
        pool="compute-pool",
        actor_class=AgentActor,
    )
    
    # 提交不同优先级的任务
    print("\n2. 提交不同优先级的任务")
    tasks = [
        (1.0, "批处理任务", "batch-job"),
        (10.0, "紧急任务", "urgent-job"),
        (5.0, "普通任务", "normal-job"),
        (2.0, "低优任务", "low-job"),
    ]
    
    for priority, name, task_id in tasks:
        scheduler.submit_task(
            label="work-queue",
            payload={"name": name},
            labels={"pool": "compute-pool"},
            priority=priority,
            task_id=task_id,
        )
        print(f"   提交: {name} (priority={priority})")
    
    # 按优先级调度
    print("\n3. 调度任务（按优先级降序）")
    while True:
        result = scheduler.choose_task("work-queue")
        if not result["success"]:
            break
        
        print(f"   调度: {result['task']['name']:15s} priority={result['priority']:4.1f} "
              f"queue_remaining={result['queue_length']}")
    
    scheduler.shutdown()
    print("\n✓ 示例完成")


def demo_fifo_same_priority():
    """演示相同优先级的 FIFO 调度"""
    print("\n" + "=" * 60)
    print("示例 2: 相同优先级按 FIFO 调度")
    print("=" * 60)
    
    scheduler = RayScheduler("fifo-demo")
    
    scheduler.create_pool(
        name="fifo-pool",
        labels={"stage": "demo"},
        resources={"cpu": 1.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_agent(
        name="fifo-worker",
        pool="fifo-pool",
        actor_class=AgentActor,
    )
    
    print("\n1. 提交5个相同优先级(5.0)的任务")
    for i in range(5):
        scheduler.submit_task(
            label="fifo-queue",
            payload={"seq": i},
            labels={"pool": "fifo-pool"},
            priority=5.0,  # 相同优先级
            task_id=f"task-{i}",
        )
        print(f"   提交: task-{i} (seq={i})")
    
    print("\n2. 调度顺序（应该按提交顺序 FIFO）")
    for expected_seq in range(5):
        result = scheduler.choose_task("fifo-queue")
        actual_seq = result["task"]["seq"]
        print(f"   调度: task-{actual_seq} (expected={expected_seq}, "
              f"match={'✓' if actual_seq == expected_seq else '✗'})")
    
    scheduler.shutdown()
    print("\n✓ 示例完成")


def demo_multiple_label_queues():
    """演示多 label 队列独立管理"""
    print("\n" + "=" * 60)
    print("示例 3: 多 label 队列独立管理")
    print("=" * 60)
    
    scheduler = RayScheduler("multi-label-demo")
    
    scheduler.create_pool(
        name="mixed-pool",
        labels={"stage": "demo"},
        resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_agent(
        name="mixed-worker",
        pool="mixed-pool",
        actor_class=AgentActor,
    )
    
    print("\n1. 向不同 label 队列提交任务")
    
    # GPU 队列
    scheduler.submit_task("gpu-work", {"type": "training"}, 
                         labels={"pool": "mixed-pool"}, priority=3.0)
    print("   提交到 gpu-work: training (priority=3.0)")
    
    scheduler.submit_task("gpu-work", {"type": "inference"}, 
                         labels={"pool": "mixed-pool"}, priority=10.0)
    print("   提交到 gpu-work: inference (priority=10.0)")
    
    # CPU 队列
    scheduler.submit_task("cpu-work", {"type": "preprocess"}, 
                         labels={"pool": "mixed-pool"}, priority=5.0)
    print("   提交到 cpu-work: preprocess (priority=5.0)")
    
    scheduler.submit_task("cpu-work", {"type": "postprocess"}, 
                         labels={"pool": "mixed-pool"}, priority=2.0)
    print("   提交到 cpu-work: postprocess (priority=2.0)")
    
    print("\n2. 从 gpu-work 队列调度（应该是 inference, priority=10.0）")
    gpu_result = scheduler.choose_task("gpu-work")
    print(f"   调度: {gpu_result['task']['type']} (priority={gpu_result['priority']})")
    
    print("\n3. 从 cpu-work 队列调度（应该是 preprocess, priority=5.0）")
    cpu_result = scheduler.choose_task("cpu-work")
    print(f"   调度: {cpu_result['task']['type']} (priority={cpu_result['priority']})")
    
    print("\n4. 各队列剩余任务")
    gpu_result2 = scheduler.choose_task("gpu-work")
    print(f"   gpu-work: {gpu_result2['task']['type']} (priority={gpu_result2['priority']})")
    
    cpu_result2 = scheduler.choose_task("cpu-work")
    print(f"   cpu-work: {cpu_result2['task']['type']} (priority={cpu_result2['priority']})")
    
    scheduler.shutdown()
    print("\n✓ 示例完成")


def demo_priority_with_preemption():
    """演示优先级调度与抢占的配合"""
    print("\n" + "=" * 60)
    print("示例 4: 优先级调度 + 抢占功能")
    print("=" * 60)
    
    scheduler = RayScheduler("preemption-priority-demo")
    
    scheduler.create_pool(
        name="preempt-pool",
        labels={"stage": "demo"},
        resources={"cpu": 1.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_agent(
        name="preempt-worker",
        pool="preempt-pool",
        actor_class=AgentActor,
    )
    
    print("\n1. 提交任务到队列")
    scheduler.submit_task("jobs", {"name": "batch"}, 
                         labels={"pool": "preempt-pool"}, 
                         priority=1.0, task_id="batch-job")
    print("   提交: batch-job (priority=1.0)")
    
    scheduler.submit_task("jobs", {"name": "online"}, 
                         labels={"pool": "preempt-pool"}, 
                         priority=5.0, task_id="online-job")
    print("   提交: online-job (priority=5.0)")
    
    print("\n2. 调度第一个任务（应该是 online-job, priority=5.0）")
    first = scheduler.choose_task("jobs")
    print(f"   调度: {first['task']['name']} (priority={first['priority']})")
    
    # 注册为运行任务
    scheduler.register_running_task(
        task_id="online-job",
        agent_name="preempt-worker",
        pool_name="preempt-pool",
        priority=5.0,
    )
    
    print("\n3. 提交紧急任务 (priority=100.0)")
    scheduler.submit_task("jobs", {"name": "emergency"}, 
                         labels={"pool": "preempt-pool"}, 
                         priority=100.0, task_id="emergency-job")
    
    print("\n4. 评估抢占")
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=100.0,
        incoming_task_pool="preempt-pool",
    )
    
    if eval_result["should_preempt"]:
        candidate = eval_result["candidates"][0]
        print(f"   可抢占任务: {candidate['task_id']} (priority={candidate['priority']})")
        print(f"   抢占得分: {candidate['preempt_score']:.2f}")
        
        # 执行抢占
        scheduler.execute_preemption(
            task_id=candidate["task_id"],
            agent_name=candidate["agent_name"],
        )
        print("   ✓ 抢占成功")
    
    print("\n5. 从队列调度下一个任务（应该是 emergency-job）")
    next_task = scheduler.choose_task("jobs")
    print(f"   调度: {next_task['task']['name']} (priority={next_task['priority']})")
    
    # 获取抢占统计
    stats = scheduler.get_preemption_stats()
    print(f"\n6. 抢占统计: 总次数={stats['total_preemptions']}, "
          f"同池={stats['same_pool_preemptions']}, 跨池={stats['cross_pool_preemptions']}")
    
    scheduler.shutdown()
    print("\n✓ 示例完成")


def main():
    """运行所有演示"""
    print("\n" + "=" * 60)
    print("ScheduleMesh 优先级调度完整演示")
    print("=" * 60)
    
    # 智能初始化 Ray
    if not ray.is_initialized():
        try:
            # 尝试连接现有集群
            ray.init(address="auto", ignore_reinit_error=True)
            print("✅ 连接到现有 Ray 集群")
        except Exception:
            # 创建新本地集群
            ray.init(ignore_reinit_error=True, local_mode=True)
            print("📋 创建新的本地 Ray 集群")
    
    try:
        demo_basic_priority_scheduling()
        demo_fifo_same_priority()
        demo_multiple_label_queues()
        demo_priority_with_preemption()
        
        print("\n" + "=" * 60)
        print("所有演示完成！")
        print("=" * 60)
        print("\n关键特性：")
        print("  ✓ 高优先级任务优先调度")
        print("  ✓ 相同优先级按 FIFO")
        print("  ✓ 多 label 队列独立管理")
        print("  ✓ 与抢占功能无缝集成")
        print("  ✓ 支持 aging 防止饥饿")
        
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()

