import time
from pathlib import Path

import pytest

from schedulemesh.core import RayScheduler
from schedulemesh.core.agent_actor import AgentActor


def test_state_recovery_and_agent_recreation(ray_runtime, tmp_path):
    state_dir = tmp_path / "scheduler-state"
    scheduler = RayScheduler(name="ha-demo", state_path=str(state_dir))

    try:
        pool_name = "ha-pool"
        scheduler.create_pool(
            name=pool_name,
            labels={"env": "test"},
            resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
            target_agents=1,
        )

        agent_result = scheduler.create_agent(
            name="ha-agent",
            pool=pool_name,
            actor_class=AgentActor,
        )
        assert agent_result["success"]

        scheduler.update_agent_metrics("ha-agent", {"timestamp": time.time()})

        scheduler.submit_task(
            label="ha-queue",
            payload={"job": "demo"},
            priority=5.0,
            task_id="job-1",
        )
    finally:
        scheduler.shutdown()

    # New scheduler instance should restore pool/queue state
    scheduler2 = RayScheduler(name="ha-demo", state_path=str(state_dir))
    try:
        pool_snapshot = scheduler2.get_pool("ha-pool")
        assert pool_snapshot["success"]

        # No heartbeat yet; ensure health check cleans up stale agent and recreates one
        health = scheduler2.ensure_agent_health(timeout=0.1)
        assert "stale" in health

        agents_after = scheduler2.list_agents().get("agents", [])
        assert agents_after, "Expected agent to be recreated"

        dispatch = scheduler2.choose_task("ha-queue")
        assert dispatch["success"], "Queued task should survive restart"
    finally:
        scheduler2.shutdown()
