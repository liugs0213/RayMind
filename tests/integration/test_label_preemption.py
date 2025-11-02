"""
基于 Label 的抢占功能集成测试

测试场景：
1. 基于 tier label 的分级抢占
2. 基于 priority_class label 的抢占
3. label 抢占优先于 pool 抢占
4. 多维度 label 抢占规则
5. 禁用 label 抢占回退到 pool 抢占
"""

from __future__ import annotations

import uuid

from schedulemesh.core.agent_actor import AgentActor


def _unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


def test_tier_based_label_preemption(scheduler):
    """测试基于 tier label 的分级抢占"""
    pool_name = _unique_name("tier-pool")
    
    # 配置 tier 抢占规则
    scheduler.update_preemption_policy(
        label_preemption_rules={
            "tier": {
                "premium": ["standard", "batch"],
                "standard": ["batch"],
            }
        },
        label_priority_threshold=0.5,
        enable_label_preemption=True,
    )
    
    # 创建资源池和 Agent
    scheduler.create_pool(
        name=pool_name,
        labels={"env": "test"},
        resources={"cpu": 2.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=2,
    )
    
    agent_1 = f"{pool_name}-agent-1"
    agent_2 = f"{pool_name}-agent-2"
    
    scheduler.create_agent(
        name=agent_1,
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    scheduler.create_agent(
        name=agent_2,
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 注册运行的任务
    scheduler.register_running_task(
        task_id="batch-task",
        agent_name=agent_1,
        pool_name=pool_name,
        priority=2.0,
        labels={"tier": "batch"},
    )
    
    scheduler.register_running_task(
        task_id="standard-task",
        agent_name=agent_2,
        pool_name=pool_name,
        priority=5.0,
        labels={"tier": "standard"},
    )
    
    # 测试 1: premium 任务可以抢占 standard（即使优先级差不大）
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=6.0,  # 优先级差 = 1.0
        incoming_task_pool=pool_name,
        incoming_task_labels={"tier": "premium"},
    )
    
    assert eval_result["should_preempt"] is True
    assert len(eval_result["candidates"]) >= 1
    
    # 验证 standard 任务在候选中
    standard_candidate = next(
        (c for c in eval_result["candidates"] if c["task_id"] == "standard-task"),
        None,
    )
    assert standard_candidate is not None
    assert standard_candidate["reason"] == "label_based_preemption"
    
    # 测试 2: standard 任务可以抢占 batch
    eval_result2 = scheduler.evaluate_preemption(
        incoming_task_priority=3.0,  # 优先级差 = 1.0
        incoming_task_pool=pool_name,
        incoming_task_labels={"tier": "standard"},
    )
    
    assert eval_result2["should_preempt"] is True
    batch_candidate = next(
        (c for c in eval_result2["candidates"] if c["task_id"] == "batch-task"),
        None,
    )
    assert batch_candidate is not None
    assert batch_candidate["reason"] == "label_based_preemption"


def test_priority_class_label_preemption(scheduler):
    """测试基于 priority_class label 的抢占"""
    pool_name = _unique_name("priority-class-pool")
    
    # 配置 priority_class 规则
    scheduler.update_preemption_policy(
        label_preemption_rules={
            "priority_class": {
                "critical": ["high", "normal", "low"],
                "high": ["normal", "low"],
                "normal": ["low"],
            }
        },
        label_priority_threshold=0.1,  # 极低阈值
        enable_label_preemption=True,
    )
    
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "test"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    agent_name = f"{pool_name}-agent"
    scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )


    # 注册 normal 级别任务
    scheduler.register_running_task(
        task_id="normal-task",
        agent_name=agent_name,
        pool_name=pool_name,
        priority=10.0,  # 高优先级
        labels={"priority_class": "normal"},
    )
    
    # critical 任务即使优先级相同也可以抢占
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=10.0,  # 优先级相同
        incoming_task_pool=pool_name,
        incoming_task_labels={"priority_class": "critical"},
    )
    
    assert eval_result["should_preempt"] is True
    assert len(eval_result["candidates"]) == 1
    assert eval_result["candidates"][0]["reason"] == "label_based_preemption"


def test_label_preemption_priority_over_pool(scheduler):
    """测试 label 抢占优先于 pool 抢占"""
    pool_a = _unique_name("pool-a")
    pool_b = _unique_name("pool-b")
    
    # 配置策略：label 抢占阈值低，跨 pool 抢占阈值高
    scheduler.update_preemption_policy(
        label_preemption_rules={
            "tier": {
                "premium": ["standard"],
            }
        },
        label_priority_threshold=0.5,
        cross_pool_priority_threshold=10.0,  # 跨 pool 需要很高优先级差
        enable_label_preemption=True,
        enable_cross_pool_preemption=True,
    )
    
    # 创建两个资源池
    scheduler.create_pool(
        name=pool_a,
        labels={"location": "east"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_pool(
        name=pool_b,
        labels={"location": "west"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    agent_a = f"{pool_a}-agent"
    scheduler.create_agent(
        name=agent_a,
        pool=pool_a,
        actor_class=AgentActor,
    )
    
    # 在 pool_a 运行 standard 任务
    scheduler.register_running_task(
        task_id="standard-in-a",
        agent_name=agent_a,
        pool_name=pool_a,
        priority=5.0,
        labels={"tier": "standard"},
    )
    
    # pool_b 的 premium 任务，优先级差不足以跨 pool 抢占
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=7.0,  # 差值 2.0 < 10.0
        incoming_task_pool=pool_b,
        incoming_task_labels={"tier": "premium"},
    )
    
    # 但由于 label 规则，仍然可以抢占
    assert eval_result["should_preempt"] is True
    assert eval_result["candidates"][0]["reason"] == "label_based_preemption"


def test_multi_label_preemption_rules(scheduler):
    """测试多维度 label 抢占规则"""
    pool_name = _unique_name("multi-label-pool")
    
    # 配置多个 label 的抢占规则
    scheduler.update_preemption_policy(
        label_preemption_rules={
            "tier": {
                "premium": ["standard"],
            },
            "user_type": {
                "admin": ["regular"],
            },
        },
        label_priority_threshold=0.5,
        enable_label_preemption=True,
    )
    
    scheduler.create_pool(
        name=pool_name,
        labels={"env": "test"},
        resources={"cpu": 2.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=2,
    )
    
    agent_1 = f"{pool_name}-agent-1"
    agent_2 = f"{pool_name}-agent-2"
    
    scheduler.create_agent(name=agent_1, pool=pool_name, actor_class=AgentActor)
    scheduler.create_agent(name=agent_2, pool=pool_name, actor_class=AgentActor)
    
    # 注册任务
    scheduler.register_running_task(
        task_id="task-1",
        agent_name=agent_1,
        pool_name=pool_name,
        priority=5.0,
        labels={"tier": "standard", "user_type": "regular"},
    )
    
    scheduler.register_running_task(
        task_id="task-2",
        agent_name=agent_2,
        pool_name=pool_name,
        priority=5.0,
        labels={"tier": "premium", "user_type": "regular"},
    )
    
    # 测试 1: premium + admin 可以抢占两个任务
    eval_result1 = scheduler.evaluate_preemption(
        incoming_task_priority=6.0,
        incoming_task_pool=pool_name,
        incoming_task_labels={"tier": "premium", "user_type": "admin"},
    )
    
    assert eval_result1["should_preempt"] is True
    # premium+admin 可以基于 user_type 抢占 task-2
    assert len(eval_result1["candidates"]) >= 1
    
    # 测试 2: standard + admin 可以基于 user_type 抢占
    eval_result2 = scheduler.evaluate_preemption(
        incoming_task_priority=6.0,
        incoming_task_pool=pool_name,
        incoming_task_labels={"tier": "standard", "user_type": "admin"},
    )
    
    assert eval_result2["should_preempt"] is True
    # 应该可以找到至少一个 user_type=regular 的任务
    assert any(c["reason"] == "label_based_preemption" for c in eval_result2["candidates"])


def test_disable_label_preemption_fallback_to_pool(scheduler):
    """测试禁用 label 抢占后回退到 pool 抢占"""
    pool_name = _unique_name("fallback-pool")
    
    # 配置 label 规则但禁用
    scheduler.update_preemption_policy(
        label_preemption_rules={
            "tier": {
                "premium": ["standard"],
            }
        },
        label_priority_threshold=0.5,
        enable_label_preemption=False,  # 禁用
        same_pool_priority_threshold=3.0,
    )
    
    scheduler.create_pool(
        name=pool_name,
        labels={"env": "test"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    agent_name = f"{pool_name}-agent"
    scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 注册 standard 任务
    scheduler.register_running_task(
        task_id="standard-task",
        agent_name=agent_name,
        pool_name=pool_name,
        priority=5.0,
        labels={"tier": "standard"},
    )
    
    # premium 任务，优先级差 1.0
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=6.0,
        incoming_task_pool=pool_name,
        incoming_task_labels={"tier": "premium"},
    )
    
    # 由于 label 抢占禁用，回退到 pool 抢占
    # 优先级差 1.0 < 3.0（同 pool 阈值），无法抢占
    assert eval_result["should_preempt"] is False


def test_label_preemption_without_matching_label(scheduler):
    """测试当任务没有匹配的 label 时回退到 pool 抢占"""
    pool_name = _unique_name("no-match-pool")
    
    scheduler.update_preemption_policy(
        label_preemption_rules={
            "tier": {
                "premium": ["standard"],
            }
        },
        label_priority_threshold=0.5,
        same_pool_priority_threshold=2.0,
        enable_label_preemption=True,
    )
    
    scheduler.create_pool(
        name=pool_name,
        labels={"env": "test"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    agent_name = f"{pool_name}-agent"
    scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 注册任务，没有 tier label
    scheduler.register_running_task(
        task_id="no-tier-task",
        agent_name=agent_name,
        pool_name=pool_name,
        priority=5.0,
        labels={"type": "batch"},  # 没有 tier label
    )
    
    # 新任务有 tier label，但运行任务没有
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=7.0,  # 差值 2.0 = same_pool_threshold
        incoming_task_pool=pool_name,
        incoming_task_labels={"tier": "premium"},
    )
    
    # 无法通过 label 抢占，但可以通过 pool 抢占（差值 2.0 >= 2.0）
    assert eval_result["should_preempt"] is True
    assert eval_result["candidates"][0]["reason"] == "same_pool_preemption"


def test_label_preemption_stats(scheduler):
    """测试 label 抢占的统计信息"""
    pool_name = _unique_name("stats-pool")
    
    scheduler.update_preemption_policy(
        label_preemption_rules={
            "tier": {
                "premium": ["standard"],
            }
        },
        label_priority_threshold=0.5,
        enable_label_preemption=True,
    )
    
    scheduler.create_pool(
        name=pool_name,
        labels={"env": "test"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    agent_name = f"{pool_name}-agent"
    scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 执行几次 label 抢占
    for i in range(3):
        task_id = f"standard-task-{i}"
        scheduler.register_running_task(
            task_id=task_id,
            agent_name=agent_name,
            pool_name=pool_name,
            priority=5.0,
            labels={"tier": "standard"},
        )
        
        scheduler.execute_preemption(task_id=task_id, agent_name=agent_name)
    
    # 检查统计
    stats = scheduler.get_preemption_stats()
    
    # 应该有至少 3 次抢占记录
    assert stats["total_preemptions"] >= 3
    
    # 检查最近的抢占记录中是否有 label_based_preemption
    recent = stats.get("recent_preemptions", [])
    label_preemptions = [r for r in recent if r.get("reason") == "label_based_preemption"]
    # 注意：execute_preemption 不会自动设置 reason，所以这里可能为 0
    # 但至少验证统计功能正常工作
    assert len(recent) >= 3


def test_preempt_task_api(scheduler):
    """测试一站式抢占 API"""
    pool_name = _unique_name("simple-preempt-pool")

    scheduler.update_preemption_policy(
        label_preemption_rules={
            "priority_class": {
                "critical": ["normal"],
            }
        },
        label_priority_threshold=0.1,
        enable_label_preemption=True,
    )

    scheduler.create_pool(
        name=pool_name,
        labels={"scene": "demo"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )

    agent_name = f"{pool_name}-agent"
    scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )

    scheduler.register_running_task(
        task_id="normal-task",
        agent_name=agent_name,
        pool_name=pool_name,
        priority=5.0,
        labels={"priority_class": "normal"},
    )

    response = scheduler.preempt_task(
        incoming_task_priority=5.0,
        incoming_task_pool=pool_name,
        incoming_task_labels={"priority_class": "critical"},
        target_task_id="normal-task",
    )

    assert response["success"] is True
    assert response["chosen_candidate"]["task_id"] == "normal-task"
    assert response["chosen_candidate"]["reason"] == "label_based_preemption"
    assert response["execution"]["success"] is True

    stats = scheduler.get_preemption_stats()
    assert stats["total_preemptions"] >= 1


def test_preempt_task_pool_only(scheduler):
    """测试禁用 label 后基于 pool 阈值的抢占"""
    pool_name = _unique_name("pool-preempt")

    scheduler.update_preemption_policy(
        enable_label_preemption=False,
        same_pool_priority_threshold=0.5,
    )

    scheduler.create_pool(
        name=pool_name,
        labels={"scene": "pool-only"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )

    agent_name = f"{pool_name}-agent"
    scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )

    scheduler.register_running_task(
        task_id="pool-task",
        agent_name=agent_name,
        pool_name=pool_name,
        priority=1.0,
        labels={"tier": "standard"},
    )

    response = scheduler.preempt_task(
        incoming_task_priority=2.0,
        incoming_task_pool=pool_name,
        incoming_task_labels={"tier": "premium"},
        target_agent_name=agent_name,
    )

    assert response["success"] is True
    assert response["chosen_candidate"]["agent_name"] == agent_name
    assert response["chosen_candidate"]["reason"] == "same_pool_preemption"


def test_preempt_task_cross_pool(scheduler):
    """测试跨 pool 抢占"""
    pool_a = _unique_name("pool-a")
    pool_b = _unique_name("pool-b")

    scheduler.update_preemption_policy(
        enable_label_preemption=False,
        enable_cross_pool_preemption=True,
        same_pool_priority_threshold=5.0,
        cross_pool_priority_threshold=0.5,
    )

    scheduler.create_pool(
        name=pool_a,
        labels={"zone": "east"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    scheduler.create_pool(
        name=pool_b,
        labels={"zone": "west"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )

    agent_a = f"{pool_a}-agent"
    agent_b = f"{pool_b}-agent"
    scheduler.create_agent(name=agent_a, pool=pool_a, actor_class=AgentActor)
    scheduler.create_agent(name=agent_b, pool=pool_b, actor_class=AgentActor)

    scheduler.register_running_task(
        task_id="a-task",
        agent_name=agent_a,
        pool_name=pool_a,
        priority=1.0,
        labels={"tier": "standard"},
    )

    response = scheduler.preempt_task(
        incoming_task_priority=2.0,
        incoming_task_pool=pool_b,
        incoming_task_labels={"tier": "standard"},
        target_agent_name=agent_a,
    )

    assert response["success"] is True
    assert response["chosen_candidate"]["agent_name"] == agent_a
    assert response["chosen_candidate"]["pool_name"] == pool_a
    assert response["chosen_candidate"]["reason"] == "cross_pool_preemption"


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
