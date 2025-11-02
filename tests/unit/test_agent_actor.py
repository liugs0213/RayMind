import time

import pytest

import ray

from schedulemesh.core.agent_actor import AgentActor


@pytest.fixture
def agent(ray_runtime):
    actor = AgentActor.options(name="test-agent", max_concurrency=10).remote("test-agent", {"stage": "unit"})
    try:
        yield actor
    finally:
        ray.kill(actor, no_restart=True)


def test_process_without_task_id(agent):
    """没有 task_id 时应立即完成且不记录运行任务。"""
    result = ray.get(agent.process.remote(payload={"value": 1}))
    assert result["method"] == "process"
    state = ray.get(agent.get_running_tasks.remote())
    assert state["count"] == 0


def test_process_with_task_and_auto_complete(agent):
    """带 task_id 但不模拟耗时时，任务应同步完成。"""
    task_id = "sync-task"
    ray.get(agent.process.remote(payload={"value": 1}, task_id=task_id, priority=0.5))
    state = ray.get(agent.get_running_tasks.remote())
    assert state["count"] == 0


def test_process_with_simulated_duration_and_cancel(agent):
    """模拟耗时时，任务应存在于运行列表，可被 cancel 移除。"""
    task_id = "long-task"
    ray.get(
        agent.process.remote(
            payload={"value": 2}, task_id=task_id, priority=1.0, simulated_duration=2.0
        )
    )

    # 等待调度器线程启动
    time.sleep(0.2)
    state = ray.get(agent.get_running_tasks.remote())
    assert state["count"] == 1
    assert state["running_tasks"][0]["task_id"] == task_id

    cancel_result = ray.get(agent.cancel.remote(task_id))
    assert cancel_result["success"] is True
    assert cancel_result["task_info"]["task_id"] == task_id

    state_after = ray.get(agent.get_running_tasks.remote())
    assert state_after["count"] == 0


def test_cancel_nonexistent_task(agent):
    """取消不存在任务应返回失败并不抛异常。"""
    result = ray.get(agent.cancel.remote("missing-task"))
    assert result["success"] is False
    assert "not found" in result["error"]
