"""
ScheduleMesh head-node helper.
"""

from __future__ import annotations

import logging
from typing import Optional

import ray

from .control.supervisor import RaySchedulerSupervisorActor
from .management.config import ActorConfig

logger = logging.getLogger(__name__)


class ScheduleMeshHead:
    """Convenience wrapper to start/stop the supervisor actor on the head node."""

    def __init__(self, name: str = "schedulemesh-supervisor"):
        self.name = name
        self._actor: Optional[ray.actor.ActorHandle] = None

    def start(self) -> bool:
        if self._actor is None:
            self._actor = RaySchedulerSupervisorActor.remote(ActorConfig(name=self.name))
            ray.get(self._actor.bootstrap.remote())
            logger.info("ScheduleMesh supervisor started (%s)", self.name)
        return True

    def stop(self) -> bool:
        if self._actor:
            ray.kill(self._actor, no_restart=True)
            self._actor = None
            logger.info("ScheduleMesh supervisor stopped (%s)", self.name)
        return True
