"""
Integration tests for RayScheduler resource and agent lifecycle.
"""

from __future__ import annotations

import uuid

import pytest
import ray

from schedulemesh.core.agent_actor import AgentActor


def _unique_pool_name() -> str:
    return f"pool-{uuid.uuid4().hex[:8]}"


def test_pool_capacity_and_default_agent(scheduler):
    pool_name = _unique_pool_name()
    pool_response = scheduler.create_pool(
        name=pool_name,
        labels={"stage": "demo"},
        resources={"cpu": 1.0, "memory": 1024.0, "gpu": 0.0, "custom": {"accelerator": 0.5}},
        target_agents=2,
    )
    assert pool_response["success"] is True
    assert pool_response["pool"]["capacity"]["cpu"] == pytest.approx(2.0)
    assert pool_response["pool"]["capacity"]["custom"]["accelerator"] == pytest.approx(1.0)

    agent_response = scheduler.create_agent(
        name="agent-default",
        pool=pool_name,
        actor_class=AgentActor,
    )
    assert agent_response["success"] is True
    pool_after_agent = scheduler.get_pool(pool_name)
    assert pool_after_agent["pool"]["used_resources"]["cpu"] == pytest.approx(1.0)
    assert pool_after_agent["pool"]["used_resources"]["custom"]["accelerator"] == pytest.approx(0.5)

    agent_info = agent_response["agent"]
    options = agent_info["options"]
    assert options["num_cpus"] == pytest.approx(1.0)
    assert options["memory"] == pytest.approx(1024.0 * 1024 * 1024)
    assert options["resources"]["accelerator"] == pytest.approx(0.5)

    handle = agent_info["handle"]
    result = ray.get(handle.invoke.remote("process", payload="hello"))
    assert result["method"] == "process"
    assert result["kwargs"]["payload"] == "hello"

    pool_state = scheduler.get_pool(pool_name)
    assert pool_state["success"] is True
    assert pool_state["pool"]["used_resources"]["cpu"] == pytest.approx(1.0)
    assert pool_state["pool"]["used_resources"]["custom"]["accelerator"] == pytest.approx(0.5)

    listed = scheduler.list_agents(pool_name)
    assert listed["success"] is True
    assert {agent["name"] for agent in listed["agents"]} == {"agent-default"}


def test_custom_agent_and_lifecycle(scheduler):
    pool_name = _unique_pool_name()
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "test"},
        resources={"cpu": 1.0, "memory": 1024.0, "gpu": 0.0, "custom": {"accelerator": 0.5}},
        target_agents=2,
    )

    scheduler.create_agent(
        name="agent-a",
        pool=pool_name,
        actor_class=AgentActor,
    )

    agent_b = scheduler.create_agent(
        name="agent-b",
        pool=pool_name,
        actor_class=AgentActor,
        resources={"cpu": 0.5, "memory": 512.0, "gpu": 0.0, "custom": {"accelerator": 0.25}},
        ray_options={"max_restarts": 1},
    )
    assert agent_b["success"] is True
    options = agent_b["agent"]["options"]
    assert options["num_cpus"] == pytest.approx(0.5)
    assert options["memory"] == pytest.approx(512.0 * 1024 * 1024)
    assert options["resources"]["accelerator"] == pytest.approx(0.25)

    exhaustion = scheduler.create_agent(
        name="agent-c",
        pool=pool_name,
        actor_class=AgentActor,
        resources={"cpu": 1.0, "memory": 1024.0, "gpu": 0.0, "custom": {"accelerator": 1.0}},
    )
    assert exhaustion["success"] is False

    deletion = scheduler.delete_agent("agent-b")
    assert deletion["success"] is True
    assert deletion["agent"]["status"] == "deleted"

    pool_state_after_delete = scheduler.get_pool(pool_name)
    assert pool_state_after_delete["pool"]["used_resources"]["cpu"] == pytest.approx(1.0)
    assert pool_state_after_delete["pool"]["used_resources"]["custom"]["accelerator"] == pytest.approx(0.5)

    recreate = scheduler.create_agent(
        name="agent-c",
        pool=pool_name,
        actor_class=AgentActor,
        resources={"cpu": 1.0, "memory": 1024.0, "gpu": 0.0, "custom": {"accelerator": 0.5}},
    )
    assert recreate["success"] is True
    pool_state_after = scheduler.get_pool(pool_name)
    assert pool_state_after["pool"]["used_resources"]["cpu"] == pytest.approx(2.0)
    assert pool_state_after["pool"]["used_resources"]["custom"]["accelerator"] == pytest.approx(1.0)

    listed = scheduler.list_agents(pool_name)
    assert {agent["name"] for agent in listed["agents"]} == {"agent-a", "agent-c"}

    scheduler.delete_agent("agent-c")
    scheduler.delete_agent("agent-a")

    post_cleanup = scheduler.list_agents(pool_name)
    assert post_cleanup["agents"] == []
