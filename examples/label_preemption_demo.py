#!/usr/bin/env python3
"""
基于 Label 的抢占功能演示

展示：
1. 基于 tier label 的分级抢占（premium > standard > batch）
2. 基于 priority_class label 的抢占
3. 基于 user label 的租户隔离抢占
4. label 抢占优先于 pool 抢占
"""

import time

import ray

from schedulemesh.core import RayScheduler
from schedulemesh.core.agent_actor import AgentActor
from schedulemesh.core.actors.control.preemption_controller import PreemptionPolicy


def demo_tier_based_preemption():
    """演示基于 tier label 的分级抢占"""
    print("\n" + "=" * 60)
    print("示例 1: 基于 tier label 的分级抢占")
    print("=" * 60)
    
    scheduler = RayScheduler("tier-preemption-demo")
    
    # 配置 label 抢占规则：定义服务等级的抢占层次
    # premium（高级服务） > standard（标准服务） > batch（批处理）
    print("\n1. 配置 label 抢占规则")
    print("   tier=premium 可以抢占 tier=standard 和 tier=batch")
    print("   tier=standard 可以抢占 tier=batch")
    
    scheduler.update_preemption_policy(
        label_preemption_rules={
            "tier": {
                "premium": ["standard", "batch"],  # premium 可以抢占 standard 和 batch
                "standard": ["batch"],             # standard 只能抢占 batch
            }
        },
        label_priority_threshold=0.5,  # label 抢占只需较低的优先级差（比 pool 抢占更宽松）
        enable_label_preemption=True,  # 启用 label 级别抢占
    )
    
    # 创建资源池
    print("\n2. 创建混合资源池")
    scheduler.create_pool(
        name="mixed-pool",
        labels={"env": "production"},
        resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
        target_agents=2,
    )
    
    # 创建 Agent
    scheduler.create_agent(
        name="agent-1",
        pool="mixed-pool",
        actor_class=AgentActor,
    )
    
    scheduler.create_agent(
        name="agent-2",
        pool="mixed-pool",
        actor_class=AgentActor,
    )
    
    # 注册运行的任务：batch 级别
    print("\n3. 注册运行中的任务")
    print("   - batch-job (tier=batch, priority=1.0)")
    scheduler.register_running_task(
        task_id="batch-job",
        agent_name="agent-1",
        pool_name="mixed-pool",
        priority=1.0,
        labels={"tier": "batch", "pool": "mixed-pool"},
        estimated_duration=60.0,
    )
    
    # 注册运行的任务：standard 级别
    print("   - standard-job (tier=standard, priority=3.0)")
    scheduler.register_running_task(
        task_id="standard-job",
        agent_name="agent-2",
        pool_name="mixed-pool",
        priority=3.0,
        labels={"tier": "standard", "pool": "mixed-pool"},
        estimated_duration=30.0,
    )
    
    # 场景 1：premium 任务到达（优先级 4.0）
    # 虽然 premium 任务的优先级不是最高（4.0 vs standard:3.0）
    # 但由于 label 规则，它可以抢占 standard 和 batch 任务
    print("\n4. 场景 1: premium 任务到达 (priority=4.0)")
    print("   评估抢占...")
    
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=4.0,
        incoming_task_pool="mixed-pool",
        incoming_task_labels={"tier": "premium", "pool": "mixed-pool"},  # premium 级别
        # 不传递 incoming_task_resources，让系统自动从Pool获取默认资源
    )
    
    print(f"   - 可以抢占: {eval_result['should_preempt']}")
    print(f"   - 候选任务数: {len(eval_result['candidates'])}")
    
    if eval_result["candidates"]:
        for candidate in eval_result["candidates"]:
            print(f"     * {candidate['task_id']} "
                  f"(tier={candidate['pool_name']}, "
                  f"score={candidate['preempt_score']:.2f}, "
                  f"reason={candidate['reason']})")
        
        # 抢占第一个候选
        top_candidate = eval_result["candidates"][0]
        preempt_result = scheduler.execute_preemption(
            task_id=top_candidate["task_id"],
            agent_name=top_candidate["agent_name"],
        )
        
        if preempt_result["success"]:
            print(f"\n   ✓ 成功抢占 {top_candidate['task_id']}")
    
    # 场景 2：standard 任务到达（优先级 2.0，只能抢占 batch）
    print("\n5. 场景 2: 另一个 standard 任务到达 (priority=2.0)")
    print("   根据规则，standard 可以抢占 batch")
    
    eval_result2 = scheduler.evaluate_preemption(
        incoming_task_priority=2.0,
        incoming_task_pool="mixed-pool",
        incoming_task_labels={"tier": "standard", "pool": "mixed-pool"},
        # 不传递 incoming_task_resources，让系统自动从Pool获取默认资源
    )
    
    print(f"   - 可以抢占: {eval_result2['should_preempt']}")
    if eval_result2["candidates"]:
        for candidate in eval_result2["candidates"]:
            print(f"     * {candidate['task_id']} (reason={candidate['reason']})")
    
    # 获取统计
    stats = scheduler.get_preemption_stats()
    print(f"\n6. 抢占统计:")
    print(f"   - 总抢占次数: {stats['total_preemptions']}")
    print(f"   - Label 级别抢占: {sum(1 for r in stats.get('recent_preemptions', []) if r.get('reason') == 'label_based_preemption')}")
    
    scheduler.shutdown()
    print("\n✓ 示例完成")


def demo_priority_class_preemption():
    """演示基于 priority_class label 的抢占"""
    print("\n" + "=" * 60)
    print("示例 2: 基于 priority_class label 的抢占")
    print("=" * 60)
    
    scheduler = RayScheduler("priority-class-demo")
    
    # 配置 priority_class 抢占规则
    print("\n1. 配置 priority_class 规则")
    print("   critical > high > normal > low")
    
    scheduler.update_preemption_policy(
        label_preemption_rules={
            "priority_class": {
                "critical": ["high", "normal", "low"],
                "high": ["normal", "low"],
                "normal": ["low"],
            }
        },
        label_priority_threshold=0.1,  # 极低的优先级阈值
        enable_label_preemption=True,
    )
    
    # 创建资源池
    scheduler.create_pool(
        name="compute-pool",
        labels={"stage": "demo"},
        resources={"cpu": 1.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_agent(
        name="worker-1",
        pool="compute-pool",
        actor_class=AgentActor,
    )
    
    # 注册一个 normal 级别的任务
    print("\n2. 注册运行任务: normal 级别 (priority=5.0)")
    scheduler.register_running_task(
        task_id="normal-task",
        agent_name="worker-1",
        pool_name="compute-pool",
        priority=5.0,
        labels={"priority_class": "normal"},
        estimated_duration=20.0,
    )
    
    # critical 任务到达（即使优先级相同，也可以抢占）
    print("\n3. critical 任务到达 (priority=5.0)")
    print("   即使优先级相同，critical 也可以抢占 normal")
    
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=5.0,  # 优先级相同
        incoming_task_pool="compute-pool",
        incoming_task_labels={"priority_class": "critical"},
        # 不传递 incoming_task_resources，让系统自动从Pool获取默认资源
    )
    
    print(f"   - 可以抢占: {eval_result['should_preempt']}")
    print(f"   - 原因: 基于 priority_class label 规则")
    
    if eval_result["candidates"]:
        candidate = eval_result["candidates"][0]
        print(f"   - 候选: {candidate['task_id']} (reason={candidate['reason']})")
        
        scheduler.execute_preemption(
            task_id=candidate["task_id"],
            agent_name=candidate["agent_name"],
        )
        print("   ✓ 抢占成功")
    
    scheduler.shutdown()
    print("\n✓ 示例完成")


def demo_multi_tenant_preemption():
    """演示基于 user label 的多租户抢占隔离"""
    print("\n" + "=" * 60)
    print("示例 3: 基于 user label 的租户隔离抢占")
    print("=" * 60)
    
    scheduler = RayScheduler("multi-tenant-demo")
    
    # 配置租户抢占规则：admin 可以抢占 vip 和 regular
    print("\n1. 配置租户抢占规则")
    print("   admin 用户可以抢占 vip 和 regular 用户")
    print("   vip 用户可以抢占 regular 用户")
    print("   regular 用户之间不能互相抢占")
    
    scheduler.update_preemption_policy(
        label_preemption_rules={
            "user_tier": {
                "admin": ["vip", "regular"],
                "vip": ["regular"],
            }
        },
        label_priority_threshold=1.0,
        enable_label_preemption=True,
    )
    
    # 创建资源池
    scheduler.create_pool(
        name="shared-pool",
        labels={"type": "shared"},
        resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
        target_agents=2,
    )
    
    scheduler.create_agent(
        name="shared-agent-1",
        pool="shared-pool",
        actor_class=AgentActor,
    )
    
    scheduler.create_agent(
        name="shared-agent-2",
        pool="shared-pool",
        actor_class=AgentActor,
    )
    
    # 注册 regular 和 vip 用户的任务
    print("\n2. 注册运行中的任务")
    print("   - regular 用户任务 (priority=5.0)")
    scheduler.register_running_task(
        task_id="regular-task",
        agent_name="shared-agent-1",
        pool_name="shared-pool",
        priority=5.0,
        labels={"user_tier": "regular"},
    )
    
    print("   - vip 用户任务 (priority=7.0)")
    scheduler.register_running_task(
        task_id="vip-task",
        agent_name="shared-agent-2",
        pool_name="shared-pool",
        priority=7.0,
        labels={"user_tier": "vip"},
    )
    
    # 场景 1：另一个 regular 用户任务（高优先级）
    print("\n3. 场景 1: 另一个 regular 用户任务 (priority=10.0)")
    print("   regular 用户之间不能互相抢占")
    
    eval_result1 = scheduler.evaluate_preemption(
        incoming_task_priority=10.0,  # 优先级很高
        incoming_task_pool="shared-pool",
        incoming_task_labels={"user_tier": "regular"},
        # 不传递 incoming_task_resources，让系统自动从Pool获取默认资源
    )
    
    print(f"   - 可以抢占: {eval_result1['should_preempt']}")
    print(f"   - 原因: label 规则不允许，回退到 pool 级别检查")
    
    # 场景 2：admin 用户任务（中等优先级）
    print("\n4. 场景 2: admin 用户任务 (priority=6.0)")
    print("   admin 可以抢占所有其他用户")
    
    eval_result2 = scheduler.evaluate_preemption(
        incoming_task_priority=6.0,  # 优先级中等
        incoming_task_pool="shared-pool",
        incoming_task_labels={"user_tier": "admin"},
        # 不传递 incoming_task_resources，让系统自动从Pool获取默认资源
    )
    
    print(f"   - 可以抢占: {eval_result2['should_preempt']}")
    print(f"   - 候选任务数: {len(eval_result2['candidates'])}")
    
    if eval_result2["candidates"]:
        for candidate in eval_result2["candidates"]:
            print(f"     * {candidate['task_id']} "
                  f"(priority={candidate['priority']}, "
                  f"reason={candidate['reason']})")
    
    scheduler.shutdown()
    print("\n✓ 示例完成")


def demo_label_vs_pool_preemption():
    """演示 label 抢占优先于 pool 抢占"""
    print("\n" + "=" * 60)
    print("示例 4: Label 抢占优先于 Pool 抢占")
    print("=" * 60)
    
    scheduler = RayScheduler("label-vs-pool-demo")
    
    # 配置策略
    print("\n1. 配置抢占策略")
    print("   - Label 抢占阈值: 0.5")
    print("   - 同 pool 抢占阈值: 3.0")
    print("   - 跨 pool 抢占阈值: 10.0")
    
    scheduler.update_preemption_policy(
        label_preemption_rules={
            "tier": {
                "premium": ["standard"],
            }
        },
        label_priority_threshold=0.5,
        same_pool_priority_threshold=3.0,
        cross_pool_priority_threshold=10.0,
        enable_label_preemption=True,
    )
    
    # 创建两个资源池
    scheduler.create_pool(
        name="pool-a",
        labels={"location": "east"},
        resources={"cpu": 1.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_pool(
        name="pool-b",
        labels={"location": "west"},
        resources={"cpu": 1.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_agent(
        name="agent-a",
        pool="pool-a",
        actor_class=AgentActor,
    )
    
    # 在 pool-a 运行 standard 任务
    print("\n2. 在 pool-a 运行 standard 任务 (priority=5.0)")
    scheduler.register_running_task(
        task_id="standard-in-a",
        agent_name="agent-a",
        pool_name="pool-a",
        priority=5.0,
        labels={"tier": "standard"},
    )
    
    # 场景：pool-b 的 premium 任务（优先级 6.0）
    print("\n3. pool-b 的 premium 任务到达 (priority=6.0)")
    print("   - 优先级差 = 1.0")
    print("   - 不满足跨 pool 抢占阈值 (10.0)")
    print("   - 但满足 label 抢占阈值 (0.5)")
    print("   - 结果：可以基于 label 规则抢占！")
    
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=6.0,
        incoming_task_pool="pool-b",  # 跨 pool
        incoming_task_labels={"tier": "premium"},
        # 不传递 incoming_task_resources，让系统自动从Pool获取默认资源
    )
    
    print(f"\n   - 可以抢占: {eval_result['should_preempt']}")
    
    if eval_result["candidates"]:
        candidate = eval_result["candidates"][0]
        print(f"   - 候选: {candidate['task_id']}")
        print(f"   - 抢占类型: {candidate['reason']}")
        print(f"   - 说明: label 抢占绕过了跨 pool 的高阈值限制")
    
    scheduler.shutdown()
    print("\n✓ 示例完成")


def main():
    """运行所有演示"""
    print("\n" + "=" * 60)
    print("ScheduleMesh 基于 Label 的抢占完整演示")
    print("=" * 60)
    
    # 智能初始化 Ray
    if not ray.is_initialized():
        try:
            # 尝试连接现有集群
            ray.init(address="auto", ignore_reinit_error=True)
            print("✅ 连接到现有 Ray 集群")
        except Exception:
            # 创建新本地集群
            ray.init(ignore_reinit_error=True)
            print("📋 创建新的本地 Ray 集群")
    
    try:
        demo_tier_based_preemption()
        time.sleep(1)
        
        demo_priority_class_preemption()
        time.sleep(1)
        
        demo_multi_tenant_preemption()
        time.sleep(1)
        
        demo_label_vs_pool_preemption()
        
        print("\n" + "=" * 60)
        print("所有演示完成！")
        print("=" * 60)
        print("\n关键特性：")
        print("  ✓ 基于 tier label 的分级抢占")
        print("  ✓ 基于 priority_class 的抢占策略")
        print("  ✓ 基于 user_tier 的租户隔离")
        print("  ✓ Label 抢占优先于 Pool 抢占")
        print("  ✓ 灵活的多维度 label 组合")
        
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()

