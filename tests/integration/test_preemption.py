"""
抢占功能集成测试

测试场景：
1. 同 pool 内抢占（高优先级任务抢占低优先级任务）
2. 跨 pool 抢占（受策略限制）
3. 保护 pool 不被抢占
4. 状态保存和恢复
"""

from __future__ import annotations

import time
import uuid

import pytest

from schedulemesh.core.agent_actor import AgentActor


def _unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


def test_same_pool_preemption(scheduler):
    """测试同 pool 内抢占：高优先级任务抢占低优先级任务"""
    pool_name = _unique_name("preempt-pool")
    
    # 创建资源池
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "preemption"},
        resources={"cpu": 2.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=2,
    )
    
    # 创建 Agent
    agent_name = f"{pool_name}-agent-0"
    agent_result = scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )
    assert agent_result["success"]
    
    # 注册一个低优先级运行任务
    low_priority_task_id = "low-priority-task"
    register_result = scheduler.register_running_task(
        task_id=low_priority_task_id,
        agent_name=agent_name,
        pool_name=pool_name,
        priority=1.0,
        labels={"pool": pool_name},
        estimated_duration=10.0,
        payload={"data": "low priority work"},
    )
    assert register_result["success"]

    scheduler.update_agent_metrics(agent_name, {"queue_length": 1.0})
    
    # 评估高优先级任务的抢占需求
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=5.0,  # 高优先级
        incoming_task_pool=pool_name,
        incoming_task_labels={"pool": pool_name},
    )
    
    assert eval_result["should_preempt"] is True
    assert len(eval_result["candidates"]) == 1
    
    candidate = eval_result["candidates"][0]
    assert candidate["task_id"] == low_priority_task_id
    assert candidate["pool_name"] == pool_name
    assert candidate["reason"] == "same_pool_preemption"
    assert candidate["preempt_score"] > 0
    
    # 执行抢占
    preempt_result = scheduler.execute_preemption(
        task_id=low_priority_task_id,
        agent_name=agent_name,
    )
    
    assert preempt_result["success"]
    assert "saved_state" in preempt_result
    assert preempt_result["saved_state"]["task_id"] == low_priority_task_id
    
    # 验证任务已从运行列表移除
    stats = scheduler.get_preemption_stats()
    assert stats["total_preemptions"] == 1
    assert stats["same_pool_preemptions"] == 1
    assert stats["cross_pool_preemptions"] == 0
    assert stats["running_tasks"] == 0


def test_cross_pool_preemption(scheduler):
    """测试跨 pool 抢占：需要更高的优先级差值"""
    pool_a = _unique_name("pool-a")
    pool_b = _unique_name("pool-b")
    
    # 创建两个资源池
    scheduler.create_pool(
        name=pool_a,
        labels={"stage": "preemption", "tier": "standard"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_pool(
        name=pool_b,
        labels={"stage": "preemption", "tier": "premium"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    # 在 pool_a 创建 Agent
    agent_a = f"{pool_a}-agent"
    scheduler.create_agent(
        name=agent_a,
        pool=pool_a,
        actor_class=AgentActor,
    )
    
    # 在 pool_a 注册一个运行任务
    task_in_pool_a = "task-pool-a"
    scheduler.register_running_task(
        task_id=task_in_pool_a,
        agent_name=agent_a,
        pool_name=pool_a,
        priority=2.0,
        labels={"pool": pool_a},
        estimated_duration=5.0,
    )
    
    # 尝试从 pool_b 抢占 pool_a 的任务 - 优先级差值不够（5.0 - 2.0 = 3.0 < 5.0）
    eval_result_low = scheduler.evaluate_preemption(
        incoming_task_priority=5.0,
        incoming_task_pool=pool_b,
        incoming_task_labels={"pool": pool_b},
    )
    
    assert eval_result_low["should_preempt"] is False
    assert len(eval_result_low["candidates"]) == 0
    
    # 使用足够高的优先级（10.0 - 2.0 = 8.0 > 5.0）
    eval_result_high = scheduler.evaluate_preemption(
        incoming_task_priority=10.0,
        incoming_task_pool=pool_b,
        incoming_task_labels={"pool": pool_b},
    )
    
    assert eval_result_high["should_preempt"] is True
    assert len(eval_result_high["candidates"]) == 1
    
    candidate = eval_result_high["candidates"][0]
    assert candidate["task_id"] == task_in_pool_a
    assert candidate["reason"] == "cross_pool_preemption"


def test_preempt_specific_agent(scheduler):
    pool_name = _unique_name("direct-preempt")

    scheduler.create_pool(
        name=pool_name,
        labels={"type": "demo"},
        resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
        target_agents=2,
    )

    agent_primary = f"{pool_name}-agent-1"
    agent_secondary = f"{pool_name}-agent-2"

    for agent in (agent_primary, agent_secondary):
        scheduler.create_agent(
            name=agent,
            pool=pool_name,
            actor_class=AgentActor,
        )

    scheduler.register_running_task(
        task_id="primary-task",
        agent_name=agent_primary,
        pool_name=pool_name,
        priority=1.0,
        labels={"pool": pool_name},
    )

    scheduler.register_running_task(
        task_id="secondary-task",
        agent_name=agent_secondary,
        pool_name=pool_name,
        priority=1.0,
        labels={"pool": pool_name},
    )

    agents = scheduler.list_agents(pool_name)["agents"]
    secondary_info = next(a for a in agents if a["name"] == agent_secondary)
    assert secondary_info["resources"]["cpu"] == 2.0

    result = scheduler.preempt_task(
        incoming_task_priority=10.0,
        incoming_task_pool=pool_name,
        target_agent_name=agent_secondary,
    )

    assert result["success"] is True
    assert result["chosen_candidate"]["agent_name"] == agent_secondary
    assert result["chosen_candidate"]["task_id"] == "secondary-task"


def test_protected_pool_not_preempted(scheduler):
    """测试被保护的 pool 不能被跨 pool 抢占"""
    protected_pool = _unique_name("protected-pool")
    attacker_pool = _unique_name("attacker-pool")
    
    # 设置抢占策略：保护特定 pool
    scheduler.update_preemption_policy(
        protected_pools=[protected_pool],
    )
    
    # 创建被保护的资源池
    scheduler.create_pool(
        name=protected_pool,
        labels={"stage": "preemption", "protected": "true"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    # 创建攻击者池
    scheduler.create_pool(
        name=attacker_pool,
        labels={"stage": "preemption"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    # 在被保护池创建任务
    agent_protected = f"{protected_pool}-agent"
    scheduler.create_agent(
        name=agent_protected,
        pool=protected_pool,
        actor_class=AgentActor,
    )
    
    task_protected = "protected-task"
    scheduler.register_running_task(
        task_id=task_protected,
        agent_name=agent_protected,
        pool_name=protected_pool,
        priority=1.0,
        labels={"pool": protected_pool},
    )
    
    # 尝试从攻击者池抢占被保护池的任务 - 应该失败
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=100.0,  # 即使优先级很高
        incoming_task_pool=attacker_pool,
        incoming_task_labels={"pool": attacker_pool},
    )
    
    assert eval_result["should_preempt"] is False
    assert len(eval_result["candidates"]) == 0


def test_preemption_with_remaining_time(scheduler):
    """测试抢占评分算法考虑剩余时间"""
    pool_name = _unique_name("time-pool")
    
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "preemption"},
        resources={"cpu": 2.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=2,
    )
    
    agent_name = f"{pool_name}-agent"
    scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 注册两个任务：一个快完成，一个刚开始
    task_almost_done = "task-almost-done"
    scheduler.register_running_task(
        task_id=task_almost_done,
        agent_name=agent_name,
        pool_name=pool_name,
        priority=2.0,
        estimated_duration=1.0,  # 很快就完成
    )
    
    # 等待一点时间让第一个任务接近完成
    time.sleep(0.5)
    
    task_just_started = "task-just-started"
    agent_name_2 = f"{pool_name}-agent-2"
    scheduler.create_agent(
        name=agent_name_2,
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    scheduler.register_running_task(
        task_id=task_just_started,
        agent_name=agent_name_2,
        pool_name=pool_name,
        priority=2.0,
        estimated_duration=10.0,  # 需要很长时间
    )
    
    # 评估抢占
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=5.0,
        incoming_task_pool=pool_name,
    )
    
    assert eval_result["should_preempt"] is True
    assert len(eval_result["candidates"]) == 2
    
    # 验证抢占得分：剩余时间越长得分越高
    # 因为公式是：Δpriority + κ × remaining_time
    candidates = eval_result["candidates"]
    # 候选按得分降序排列，所以第一个应该是刚开始的任务（剩余时间长，但得分反而高）
    assert candidates[0]["task_id"] == task_just_started


def test_preemption_stats(scheduler):
    """测试抢占统计功能"""
    pool_name = _unique_name("stats-pool")
    
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "preemption"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    agent_name = f"{pool_name}-agent"
    scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 初始统计
    stats_initial = scheduler.get_preemption_stats()
    initial_count = stats_initial["total_preemptions"]
    
    # 执行几次抢占
    for i in range(3):
        task_id = f"task-{i}"
        scheduler.register_running_task(
            task_id=task_id,
            agent_name=agent_name,
            pool_name=pool_name,
            priority=1.0,
        )
        
        scheduler.execute_preemption(task_id=task_id, agent_name=agent_name)
    
    # 检查统计
    stats_final = scheduler.get_preemption_stats()
    assert stats_final["total_preemptions"] == initial_count + 3
    assert stats_final["running_tasks"] == 0
    assert len(stats_final["recent_preemptions"]) >= 3


def test_disable_cross_pool_preemption(scheduler):
    """测试禁用跨 pool 抢占"""
    pool_a = _unique_name("pool-a")
    pool_b = _unique_name("pool-b")
    
    # 禁用跨 pool 抢占
    scheduler.update_preemption_policy(
        enable_cross_pool_preemption=False,
    )
    
    # 创建两个池
    scheduler.create_pool(
        name=pool_a,
        labels={"stage": "preemption"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_pool(
        name=pool_b,
        labels={"stage": "preemption"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    agent_a = f"{pool_a}-agent"
    scheduler.create_agent(
        name=agent_a,
        pool=pool_a,
        actor_class=AgentActor,
    )
    
    task_a = "task-in-pool-a"
    scheduler.register_running_task(
        task_id=task_a,
        agent_name=agent_a,
        pool_name=pool_a,
        priority=1.0,
    )
    
    # 尝试跨 pool 抢占 - 应该失败
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=100.0,  # 即使优先级很高
        incoming_task_pool=pool_b,
    )
    
    assert eval_result["should_preempt"] is False
    assert len(eval_result["candidates"]) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
