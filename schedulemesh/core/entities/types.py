"""
Common type definitions shared across actors and controllers.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from schedulemesh.core.entities.resource_pool import ResourceSpec


@dataclass
class CancelContext:
    task_id: str
    agent_name: str
    metadata: Dict[str, Any]


@dataclass
class RestoreContext:
    task_id: str
    agent_name: str
    state_ref: Optional[Any] = None


@dataclass
class SavedState:
    payload: Any
    timestamp: float


@dataclass
class TaskContext:
    task_id: str
    labels: Dict[str, str]
    priority: float


@dataclass
class Task:
    context: TaskContext
    payload_ref: Any


@dataclass
class ClusterResourceSnapshot:
    """Captured view of aggregate cluster resources."""

    total: ResourceSpec
    available: ResourceSpec


class AgentStatus(str, Enum):
    """Lifecycle status values reported by AgentManager."""

    RUNNING = "running"
    TERMINATING = "terminating"
    DELETED = "deleted"
    ERROR = "error"
