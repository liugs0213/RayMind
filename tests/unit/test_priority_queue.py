"""
优先级队列单元测试

测试：
1. 基本优先级排序
2. 相同优先级时的 FIFO 行为
3. Aging 机制防止饥饿
4. 队列操作（push, pop, peek）
"""

from __future__ import annotations

import time

import pytest

from schedulemesh.core.actors.control.scheduler_actor import PriorityTaskQueue


def test_basic_priority_ordering():
    """测试基本优先级排序：高优先级任务先出队"""
    queue = PriorityTaskQueue(aging_factor=0.0)  # 禁用 aging
    
    # 按不同顺序插入任务
    queue.push({"name": "low"}, priority=1.0)
    queue.push({"name": "high"}, priority=10.0)
    queue.push({"name": "medium"}, priority=5.0)
    
    # 应该按优先级降序出队
    assert queue.pop()["name"] == "high"   # 10.0
    assert queue.pop()["name"] == "medium"  # 5.0
    assert queue.pop()["name"] == "low"    # 1.0
    assert queue.pop() is None  # 队列空


def test_same_priority_fifo():
    """测试相同优先级时的 FIFO 行为"""
    queue = PriorityTaskQueue(aging_factor=0.0)
    
    # 插入相同优先级的任务
    queue.push({"name": "first", "id": 1}, priority=5.0)
    queue.push({"name": "second", "id": 2}, priority=5.0)
    queue.push({"name": "third", "id": 3}, priority=5.0)
    
    # 应该按插入顺序出队（FIFO）
    assert queue.pop()["id"] == 1
    assert queue.pop()["id"] == 2
    assert queue.pop()["id"] == 3


def test_aging_mechanism():
    """测试 aging 机制：低优先级任务等待久了会提升优先级"""
    # aging_factor = 1.0 意味着每等待1秒，优先级 +1.0
    queue = PriorityTaskQueue(aging_factor=1.0)
    
    # 插入低优先级任务
    base_time = time.time()
    queue.push({"name": "old-low"}, priority=1.0, submit_time=base_time - 10)  # 10秒前
    
    # 插入高优先级任务（刚提交）
    queue.push({"name": "new-high"}, priority=5.0, submit_time=base_time)
    
    # 有效优先级计算：
    # old-low: 1.0 + 1.0 × 10 = 11.0
    # new-high: 5.0 + 1.0 × 0 = 5.0
    # 所以 old-low 应该先出队
    assert queue.pop()["name"] == "old-low"
    assert queue.pop()["name"] == "new-high"


def test_peek_does_not_remove():
    """测试 peek 不会移除任务"""
    queue = PriorityTaskQueue()
    
    queue.push({"name": "task1"}, priority=10.0)
    queue.push({"name": "task2"}, priority=5.0)
    
    # peek 多次
    assert queue.peek()["name"] == "task1"
    assert queue.peek()["name"] == "task1"
    assert len(queue) == 2  # 长度不变
    
    # pop 会移除
    assert queue.pop()["name"] == "task1"
    assert len(queue) == 1


def test_queue_length():
    """测试队列长度操作"""
    queue = PriorityTaskQueue()
    
    assert len(queue) == 0
    assert queue.is_empty()
    assert not queue  # __bool__
    
    queue.push({"name": "task1"}, priority=1.0)
    assert len(queue) == 1
    assert not queue.is_empty()
    assert queue
    
    queue.push({"name": "task2"}, priority=2.0)
    assert len(queue) == 2
    
    queue.pop()
    assert len(queue) == 1
    
    queue.pop()
    assert len(queue) == 0
    assert queue.is_empty()


def test_metadata_removed_after_pop():
    """测试内部元数据在 pop 后被移除"""
    queue = PriorityTaskQueue()
    
    task = {"name": "test", "data": "value"}
    queue.push(task, priority=5.0)
    
    result = queue.pop()
    
    # 原始数据应该保留
    assert result["name"] == "test"
    assert result["data"] == "value"
    
    # 内部元数据应该被移除
    assert "_submit_time" not in result
    assert "_original_priority" not in result


def test_mixed_priorities():
    """测试混合优先级场景"""
    queue = PriorityTaskQueue(aging_factor=0.0)
    
    # 模拟真实场景：不同优先级的任务混合插入
    priorities_and_names = [
        (5.0, "task-a"),
        (10.0, "task-b"),
        (1.0, "task-c"),
        (10.0, "task-d"),  # 与 task-b 同优先级
        (3.0, "task-e"),
    ]
    
    for priority, name in priorities_and_names:
        queue.push({"name": name}, priority=priority)
    
    # 应该按优先级降序出队，同优先级时 FIFO
    assert queue.pop()["name"] == "task-b"  # 10.0, 先插入
    assert queue.pop()["name"] == "task-d"  # 10.0, 后插入
    assert queue.pop()["name"] == "task-a"  # 5.0
    assert queue.pop()["name"] == "task-e"  # 3.0
    assert queue.pop()["name"] == "task-c"  # 1.0


def test_empty_queue_operations():
    """测试空队列操作"""
    queue = PriorityTaskQueue()
    
    assert queue.pop() is None
    assert queue.peek() is None
    assert len(queue) == 0
    assert queue.is_empty()


def test_aging_with_small_factor():
    """测试小 aging_factor 的实际场景"""
    # 默认值 0.0001：等待 10000 秒（约 2.7 小时）才能提升 1.0 优先级
    queue = PriorityTaskQueue(aging_factor=0.0001)
    
    base_time = time.time()
    
    # 低优任务等待了很久
    queue.push({"name": "old-low"}, priority=1.0, submit_time=base_time - 50000)
    
    # 高优任务刚提交
    queue.push({"name": "new-high"}, priority=5.0, submit_time=base_time)
    
    # 有效优先级：
    # old-low: 1.0 + 0.0001 × 50000 = 6.0
    # new-high: 5.0 + 0.0001 × 0 = 5.0
    # old-low 应该先出队
    assert queue.pop()["name"] == "old-low"


def test_negative_priorities():
    """测试负优先级"""
    queue = PriorityTaskQueue(aging_factor=0.0)
    
    queue.push({"name": "negative"}, priority=-5.0)
    queue.push({"name": "zero"}, priority=0.0)
    queue.push({"name": "positive"}, priority=5.0)
    
    # 正数 > 0 > 负数
    assert queue.pop()["name"] == "positive"
    assert queue.pop()["name"] == "zero"
    assert queue.pop()["name"] == "negative"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

