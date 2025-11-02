"""Example: Persist preemption state to local files.

This script demonstrates how to register custom state handlers so that
preempted tasks are written to the local filesystem.  The saved payload can
later be restored when the task is rescheduled.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Dict

import ray

from schedulemesh.core import RayScheduler
from schedulemesh.core.agent_actor import AgentActor
from schedulemesh.core.actors.control.preemption_controller import TaskState

# Directory where we store serialized task states
CHECKPOINT_DIR = Path(tempfile.gettempdir()) / "schedulemesh-preemption"
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


def file_state_preserver(task_state: TaskState) -> Dict[str, str]:
    """Serialize TaskState to a JSON file under CHECKPOINT_DIR."""
    payload = {
        "task_id": task_state.task_id,
        "agent": task_state.agent_name,
        "pool": task_state.pool_name,
        "priority": task_state.priority,
        "labels": task_state.labels,
        "payload": task_state.payload,
    }
    path = CHECKPOINT_DIR / f"{task_state.task_id}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"task_id": task_state.task_id, "checkpoint_path": str(path)}


def file_state_restorer(saved_state: Dict[str, str]) -> None:
    """Load serialized state from disk for inspection/rehydration."""
    path = Path(saved_state["checkpoint_path"])
    if not path.exists():
        raise FileNotFoundError(f"Checkpoint {path} does not exist")
    payload = json.loads(path.read_text(encoding="utf-8"))
    print(f"Restoring task {payload['task_id']} from {path}")
    # In real scenarios you would pass this payload to the actor or another service.


def main() -> None:
    # 智能初始化 Ray
    try:
        # 尝试连接现有集群
        ray.init(address="auto", ignore_reinit_error=True)
        print("✅ 连接到现有 Ray 集群")
    except Exception:
        # 创建新本地集群
        ray.init(ignore_reinit_error=True)
        print("📋 创建新的本地 Ray 集群")
    scheduler = RayScheduler("file-preemption-demo")

    # Register file-based handlers for a pool
    scheduler.register_state_handlers(
        pool_name="file-pool",
        preserver=file_state_preserver,
        restorer=file_state_restorer,
    )

    scheduler.create_pool(
        name="file-pool",
        labels={"stage": "preemption"},
        resources={"cpu": 1.0, "memory": 1024.0, "gpu": 0.0},
        target_agents=1,
    )
    scheduler.create_agent(
        name="file-agent",
        pool="file-pool",
        actor_class=AgentActor,
    )

    scheduler.register_running_task(
        task_id="demo-task",
        agent_name="file-agent",
        pool_name="file-pool",
        priority=1.0,
        labels={"pool": "file-pool"},
        payload={"data": "example"},
    )

    result = scheduler.execute_preemption(task_id="demo-task", agent_name="file-agent")
    print("Preemption result:", result)

    saved_path = result["saved_state"]["checkpoint_path"]
    print("Saved checkpoint at:", saved_path)

    # Clean up resources
    scheduler.shutdown()
    ray.shutdown()


if __name__ == "__main__":
    main()
