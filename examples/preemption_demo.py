#!/usr/bin/env python3
"""
ScheduleMesh 抢占功能演示

展示：
1. 同 pool 内抢占
2. 跨 pool 抢占
3. 保护策略
"""

import time

import ray

from schedulemesh.core import RayScheduler
from schedulemesh.core.agent_actor import AgentActor


def demo_same_pool_preemption():
    """演示同 pool 内抢占"""
    print("\n" + "=" * 60)
    print("示例 1: 同 Pool 内抢占")
    print("=" * 60)
    
    scheduler = RayScheduler("preemption-demo")
    
    # 创建资源池
    print("\n1. 创建资源池 'compute-pool'")
    scheduler.create_pool(
        name="compute-pool",
        labels={"tier": "standard"},
        resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
        target_agents=1,
    )
    
    # 创建 Agent
    print("2. 创建 Agent")
    scheduler.create_agent(
        name="agent-1",
        pool="compute-pool",
        actor_class=AgentActor,
    )
    
    # 注册低优先级任务
    print("3. 注册低优先级任务 (priority=1.0)")
    print("   任务将自动使用Pool默认资源: CPU=2.0, Memory=2.0GB")
    scheduler.register_running_task(
        task_id="low-priority-task",
        agent_name="agent-1",
        pool_name="compute-pool",
        priority=1.0,
        labels={"pool": "compute-pool"},
        estimated_duration=30.0,
        payload={"type": "batch processing"},
        # 不传递 resources，让系统自动从Pool获取默认资源
    )
    
    # 评估抢占
    print("\n4. 高优先级任务到达 (priority=5.0)，评估抢占...")
    print("   新任务将使用Pool默认资源: CPU=2.0, Memory=2.0GB")
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=5.0,
        incoming_task_pool="compute-pool",
    )
    
    print(f"   - 是否需要抢占: {eval_result['should_preempt']}")
    print(f"   - 候选任务数: {len(eval_result['candidates'])}")
    
    if eval_result["candidates"]:
        candidate = eval_result["candidates"][0]
        print(f"\n5. 抢占候选:")
        print(f"   - 任务ID: {candidate['task_id']}")
        print(f"   - 优先级: {candidate['priority']}")
        print(f"   - 抢占得分: {candidate['preempt_score']:.2f}")
        print(f"   - 抢占类型: {candidate['reason']}")
        
        # 执行抢占
        print("\n6. 执行抢占...")
        preempt_result = scheduler.execute_preemption(
            task_id=candidate["task_id"],
            agent_name=candidate["agent_name"],
        )
        
        if preempt_result["success"]:
            print("   ✓ 抢占成功！")
            print(f"   - 状态已保存: {preempt_result['saved_state']['task_id']}")
    
    # 查看统计
    print("\n7. 抢占统计:")
    stats = scheduler.get_preemption_stats()
    print(f"   - 总抢占次数: {stats['total_preemptions']}")
    print(f"   - 同池抢占: {stats['same_pool_preemptions']}")
    print(f"   - 跨池抢占: {stats['cross_pool_preemptions']}")
    
    scheduler.shutdown()
    print("\n✓ 示例完成")


def demo_cross_pool_preemption():
    """演示跨 pool 抢占"""
    print("\n" + "=" * 60)
    print("示例 2: 跨 Pool 抢占")
    print("=" * 60)
    
    scheduler = RayScheduler("cross-pool-demo")
    
    # 创建两个资源池
    print("\n1. 创建两个资源池")
    scheduler.create_pool(
        name="standard-pool",
        labels={"tier": "standard"},
        resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_pool(
        name="premium-pool",
        labels={"tier": "premium"},
        resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
        target_agents=1,
    )
    
    # 在标准池创建 Agent 和任务
    print("2. 在 standard-pool 运行低优先级任务")
    scheduler.create_agent(
        name="standard-agent",
        pool="standard-pool",
        actor_class=AgentActor,
    )
    
    scheduler.register_running_task(
        task_id="standard-task",
        agent_name="standard-agent",
        pool_name="standard-pool",
        priority=2.0,
        estimated_duration=20.0,
        # 不传递 resources，让系统自动从Pool获取默认资源
    )
    
    # 从高级池尝试抢占 - 优先级不够
    print("\n3. 从 premium-pool 发起抢占 (priority=5.0)")
    print("   注意：跨池抢占需要更高的优先级差值 (默认 >= 5.0)")
    
    eval_low = scheduler.evaluate_preemption(
        incoming_task_priority=5.0,  # 差值 = 3.0 < 5.0
        incoming_task_pool="premium-pool",
        # 不传递 incoming_task_resources，让系统自动使用Pool的默认资源
    )
    
    print(f"   - 优先级差值: 5.0 - 2.0 = 3.0 < 5.0 (阈值)")
    print(f"   - 是否可抢占: {eval_low['should_preempt']}")
    
    # 使用更高优先级
    print("\n4. 使用更高优先级 (priority=10.0)")
    eval_high = scheduler.evaluate_preemption(
        incoming_task_priority=10.0,  # 差值 = 8.0 > 5.0
        incoming_task_pool="premium-pool",
        # 不传递 incoming_task_resources，让系统自动使用Pool的默认资源
    )
    
    print(f"   - 优先级差值: 10.0 - 2.0 = 8.0 > 5.0 (阈值)")
    print(f"   - 是否可抢占: {eval_high['should_preempt']}")
    
    if eval_high["candidates"]:
        candidate = eval_high["candidates"][0]
        print(f"   - 抢占类型: {candidate['reason']}")
        print("   ✓ 跨池抢占满足条件！")
    
    scheduler.shutdown()
    print("\n✓ 示例完成")


def demo_protected_pools():
    """演示保护策略"""
    print("\n" + "=" * 60)
    print("示例 3: 资源池保护策略")
    print("=" * 60)
    
    scheduler = RayScheduler("protected-demo")
    
    # 设置保护策略
    print("\n1. 设置 'production-pool' 为受保护池")
    scheduler.update_preemption_policy(
        protected_pools=["production-pool"],
    )
    
    # 创建生产池和普通池
    print("2. 创建资源池")
    scheduler.create_pool(
        name="production-pool",
        labels={"env": "production"},
        resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_pool(
        name="dev-pool",
        labels={"env": "development"},
        resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
        target_agents=1,
    )
    
    # 在生产池运行任务
    print("3. 在 production-pool 运行任务")
    scheduler.create_agent(
        name="prod-agent",
        pool="production-pool",
        actor_class=AgentActor,
    )
    
    scheduler.register_running_task(
        task_id="prod-service",
        agent_name="prod-agent",
        pool_name="production-pool",
        priority=5.0,
        # 不传递 resources，让系统自动从Pool获取默认资源
    )
    
    # 尝试从开发池抢占
    print("\n4. 从 dev-pool 尝试抢占生产池任务 (priority=100.0)")
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=100.0,  # 超高优先级
        incoming_task_pool="dev-pool",
        # 不传递 incoming_task_resources，让系统自动使用Pool的默认资源
    )
    
    print(f"   - 是否可抢占: {eval_result['should_preempt']}")
    print(f"   - 原因: 生产池受保护，不允许跨池抢占")
    print("   ✓ 保护策略生效！")
    
    scheduler.shutdown()
    print("\n✓ 示例完成")


def main():
    """运行所有演示"""
    print("\n" + "=" * 60)
    print("ScheduleMesh 抢占功能完整演示")
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
        # 运行各个示例
        demo_same_pool_preemption()
        time.sleep(1)
        
        demo_cross_pool_preemption()
        time.sleep(1)
        
        demo_protected_pools()
        
        print("\n" + "=" * 60)
        print("所有演示完成！")
        print("=" * 60)
        
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()

