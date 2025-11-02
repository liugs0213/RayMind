"""
Pluggable scheduling strategy interfaces.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class SchedulingStrategy(ABC):
    """Base interface for scheduling strategy plugins."""

    @abstractmethod
    def score(self, task: dict[str, Any], agent: dict[str, Any]) -> float:
        """Return a score that indicates how suitable the agent is for the task."""

    @abstractmethod
    def priority(self, task: dict[str, Any]) -> float:
        """Return the scheduling priority for the given task."""
