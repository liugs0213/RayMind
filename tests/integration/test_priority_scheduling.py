"""
优先级调度集成测试

测试场景：
1. 高优先级任务先被调度
2. 同优先级任务按 FIFO 调度
3. 不同 label 的队列独立管理
4. Aging 机制在实际调度中的效果
"""

from __future__ import annotations

import time
import uuid

import pytest

from schedulemesh.core.agent_actor import AgentActor

pytestmark = pytest.mark.skip(reason="Integration-level Ray scheduler scenarios are disabled in lightweight environments.")


def _unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


def test_high_priority_scheduled_first(scheduler):
    """测试高优先级任务先被调度"""
    pool_name = _unique_name("priority-pool")
    
    # 创建资源池和 Agent
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "priority-test"},
        resources={"cpu": 2.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=1,
    )
    
    agent_name = f"{pool_name}-agent"
    scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 提交不同优先级的任务
    task_ids = []
    priorities = [
        (1.0, "low"),
        (10.0, "high"),
        (5.0, "medium"),
    ]
    
    for priority, name in priorities:
        result = scheduler.submit_task(
            label="priority-work",
            payload={"name": name},
            labels={"pool": pool_name},
            priority=priority,
        )
        task_ids.append(result["task_id"])
    
    # 调度任务，应该按优先级降序
    first = scheduler.choose_task("priority-work")
    assert first["success"]
    assert first["task"]["name"] == "high"  # priority=10.0
    assert first["priority"] == 10.0
    
    second = scheduler.choose_task("priority-work")
    assert second["success"]
    assert second["task"]["name"] == "medium"  # priority=5.0
    assert second["priority"] == 5.0
    
    third = scheduler.choose_task("priority-work")
    assert third["success"]
    assert third["task"]["name"] == "low"  # priority=1.0
    assert third["priority"] == 1.0
    
    # 队列已空
    fourth = scheduler.choose_task("priority-work")
    assert not fourth["success"]


def test_same_priority_fifo_order(scheduler):
    """测试相同优先级时按 FIFO 顺序调度"""
    pool_name = _unique_name("fifo-pool")
    
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "fifo-test"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_agent(
        name=f"{pool_name}-agent",
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 提交相同优先级的任务
    for i in range(5):
        scheduler.submit_task(
            label="fifo-work",
            payload={"seq": i},
            labels={"pool": pool_name},
            priority=5.0,  # 所有任务相同优先级
        )
    
    # 应该按提交顺序调度
    for expected_seq in range(5):
        result = scheduler.choose_task("fifo-work")
        assert result["success"]
        assert result["task"]["seq"] == expected_seq


def test_independent_label_queues(scheduler):
    """测试不同 label 的队列独立管理"""
    pool_name = _unique_name("multi-label-pool")
    
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "multi-label"},
        resources={"cpu": 2.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_agent(
        name=f"{pool_name}-agent",
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 向不同 label 提交任务
    scheduler.submit_task(
        label="gpu-work",
        payload={"type": "gpu", "priority": 1.0},
        labels={"pool": pool_name},
        priority=1.0,
    )
    
    scheduler.submit_task(
        label="cpu-work",
        payload={"type": "cpu", "priority": 10.0},
        labels={"pool": pool_name},
        priority=10.0,
    )
    
    scheduler.submit_task(
        label="gpu-work",
        payload={"type": "gpu", "priority": 5.0},
        labels={"pool": pool_name},
        priority=5.0,
    )
    
    # 从 gpu-work 队列调度，应该是 priority=5.0 的任务
    gpu_result = scheduler.choose_task("gpu-work")
    assert gpu_result["success"]
    assert gpu_result["task"]["type"] == "gpu"
    assert gpu_result["priority"] == 5.0
    
    # 从 cpu-work 队列调度
    cpu_result = scheduler.choose_task("cpu-work")
    assert cpu_result["success"]
    assert cpu_result["task"]["type"] == "cpu"
    assert cpu_result["priority"] == 10.0
    
    # gpu-work 还有一个 priority=1.0 的任务
    gpu_result2 = scheduler.choose_task("gpu-work")
    assert gpu_result2["success"]
    assert gpu_result2["priority"] == 1.0


def test_priority_with_preemption_integration(scheduler):
    """测试优先级调度与抢占功能的集成"""
    pool_name = _unique_name("preempt-priority-pool")
    
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "preempt-test"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    agent_name = f"{pool_name}-agent"
    scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 提交多个任务到队列
    scheduler.submit_task(
        label="preempt-work",
        payload={"name": "batch-job"},
        labels={"pool": pool_name},
        priority=1.0,
        task_id="low-priority-task",
    )
    
    scheduler.submit_task(
        label="preempt-work",
        payload={"name": "critical-job"},
        labels={"pool": pool_name},
        priority=10.0,
        task_id="high-priority-task",
    )
    
    # 调度第一个任务（应该是高优先级的）
    first_task = scheduler.choose_task("preempt-work")
    assert first_task["success"]
    assert first_task["task_id"] == "high-priority-task"
    assert first_task["priority"] == 10.0
    
    # 注册为运行任务
    scheduler.register_running_task(
        task_id="high-priority-task",
        agent_name=agent_name,
        pool_name=pool_name,
        priority=10.0,
    )
    
    # 提交一个更高优先级的任务
    scheduler.submit_task(
        label="preempt-work",
        payload={"name": "emergency"},
        labels={"pool": pool_name},
        priority=100.0,
        task_id="emergency-task",
    )
    
    # 评估抢占
    eval_result = scheduler.evaluate_preemption(
        incoming_task_priority=100.0,
        incoming_task_pool=pool_name,
    )
    
    # 应该可以抢占当前运行的任务
    assert eval_result["should_preempt"]
    
    # 队列中的下一个任务应该是 emergency-task (priority=100)
    # 而不是 low-priority-task (priority=1)
    next_task = scheduler.choose_task("preempt-work")
    assert next_task["task_id"] == "emergency-task"
    assert next_task["priority"] == 100.0


def test_empty_queue_behavior(scheduler):
    """测试空队列行为"""
    result = scheduler.choose_task("non-existent-label")
    assert not result["success"]
    assert result["queue_length"] == 0


def test_no_matching_agent(scheduler):
    """测试没有匹配 Agent 时任务留在队列"""
    pool_name = _unique_name("no-match-pool")
    
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "no-match"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    # 不创建 Agent，直接提交任务
    scheduler.submit_task(
        label="orphan-work",
        payload={"data": "test"},
        labels={"pool": pool_name},  # 需要特定 pool
        priority=5.0,
    )
    
    # 尝试调度，应该失败但任务留在队列
    result = scheduler.choose_task("orphan-work")
    assert not result["success"]
    assert "No registered agents" in result["error"]
    assert result["queue_length"] == 1  # 任务还在队列中
    
    # 现在创建 Agent
    scheduler.create_agent(
        name=f"{pool_name}-agent",
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 再次调度，应该成功
    result2 = scheduler.choose_task("orphan-work")
    assert result2["success"]
    assert result2["queue_length"] == 0


def test_large_scale_priority_queue(scheduler):
    """测试大规模任务的优先级队列性能"""
    pool_name = _unique_name("scale-pool")
    
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "scale-test"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=1,
    )
    
    scheduler.create_agent(
        name=f"{pool_name}-agent",
        pool=pool_name,
        actor_class=AgentActor,
    )
    
    # 提交 100 个任务，优先级随机
    import random
    priorities = [random.uniform(0, 100) for _ in range(100)]
    
    for i, priority in enumerate(priorities):
        scheduler.submit_task(
            label="scale-work",
            payload={"index": i, "priority": priority},
            labels={"pool": pool_name},
            priority=priority,
        )
    
    # 调度所有任务，验证是按优先级降序
    previous_priority = float('inf')
    for _ in range(100):
        result = scheduler.choose_task("scale-work")
        assert result["success"]
        current_priority = result["priority"]
        assert current_priority <= previous_priority
        previous_priority = current_priority


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
