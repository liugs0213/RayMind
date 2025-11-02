"""
Integration tests for SchedulerActor task dispatch leveraging label-aware strategies.
"""

from __future__ import annotations

import uuid

import pytest

from schedulemesh.core.agent_actor import AgentActor

pytestmark = pytest.mark.skip(reason="Integration-level Ray scheduler scenarios are disabled in lightweight environments.")


def _unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


def test_scheduler_round_robin_dispatch_by_pool(scheduler):
    pool_name = _unique_name("dispatch-pool")
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "dispatch"},
        resources={"cpu": 0.5, "memory": 128.0, "gpu": 0.0},
        target_agents=2,
    )

    agent_names = []
    for index in range(2):
        name = f"{pool_name}-agent-{index}"
        scheduler.create_agent(
            name=name,
            pool=pool_name,
            actor_class=AgentActor,
        )
        agent_names.append(name)

    for idx in range(3):
        enqueue_result = scheduler.submit_task(
            label="dispatch-work",
            payload={"seq": idx},
            labels={"pool": pool_name},
            strategy="label_round_robin",
        )
        assert enqueue_result["success"] is True
        assert enqueue_result["strategy"] == "label_round_robin"

    selections = [scheduler.choose_task("dispatch-work") for _ in range(3)]

    assert all(selection["success"] for selection in selections)
    assert [selection["queue_length"] for selection in selections] == [2, 1, 0]
    assert [selection["task"]["seq"] for selection in selections] == [0, 1, 2]
    assert [selection["agent"]["name"] for selection in selections] == [
        agent_names[0],
        agent_names[1],
        agent_names[0],
    ]
    assert all(selection["agent"]["labels"]["pool"] == pool_name for selection in selections)
    assert all(selection["strategy"] == "label_round_robin" for selection in selections)


def test_scheduler_waits_for_matching_agent(scheduler):
    pool_name = _unique_name("pending-pool")
    scheduler.submit_task(
        label="pending-work",
        payload={"id": "pending"},
        labels={"pool": pool_name},
        strategy="label_round_robin",
    )

    first_attempt = scheduler.choose_task("pending-work")
    assert first_attempt["success"] is False
    assert first_attempt["queue_length"] == 1
    assert "match labels" in first_attempt["error"]

    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "pending"},
        resources={"cpu": 0.5, "memory": 128.0, "gpu": 0.0},
        target_agents=1,
    )
    agent_name = f"{pool_name}-agent"
    scheduler.create_agent(
        name=agent_name,
        pool=pool_name,
        actor_class=AgentActor,
    )

    second_attempt = scheduler.choose_task("pending-work")
    assert second_attempt["success"] is True
    assert second_attempt["queue_length"] == 0
    assert second_attempt["task"]["id"] == "pending"
    assert second_attempt["agent"]["name"] == agent_name
    assert second_attempt["agent"]["labels"]["pool"] == pool_name
    assert second_attempt["strategy"] == "label_round_robin"


def test_scheduler_respects_custom_strategy(scheduler):
    pool_name = _unique_name("custom-pool")
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "custom"},
        resources={"cpu": 0.5, "memory": 128.0, "gpu": 0.0},
        target_agents=2,
    )

    agent_names = []
    for index in range(2):
        name = f"{pool_name}-agent-{index}"
        scheduler.create_agent(
            name=name,
            pool=pool_name,
            actor_class=AgentActor,
        )
        agent_names.append(name)

    scheduler.update_agent_metrics(agent_names[0], {"queue_length": 5})
    scheduler.update_agent_metrics(agent_names[1], {"queue_length": 1})

    scheduler.submit_task(
        label="custom-work",
        payload={"seq": 0},
        labels={"pool": pool_name},
        strategy="least_busy",
    )

    selection = scheduler.choose_task("custom-work")

    assert selection["success"] is True
    assert selection["agent"]["name"] == agent_names[1]
    assert selection["strategy"] == "least_busy"
    assert selection["queue_length"] == 0
