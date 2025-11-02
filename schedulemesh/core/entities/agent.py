"""
Agent entity definitions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass
class AgentMetrics:
    """Basic metrics collected from an agent."""

    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    gpu_usage: float = 0.0
    queue_length: int = 0
    tasks_completed: int = 0


class Agent:
    """Lightweight wrapper around a Ray actor handle."""

    def __init__(self, name: str, labels: Dict[str, str]):
        self.name = name
        self.labels = labels
        self.metrics = AgentMetrics()
