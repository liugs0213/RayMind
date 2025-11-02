"""
Autoscaler actor skeleton.

This module will eventually contain logic to scale resource pools based on
metrics or scheduling feedback.
"""

from __future__ import annotations

import logging

import ray

from .config import ActorConfig

logger = logging.getLogger(__name__)


@ray.remote
class AutoscalerActor:
    """Placeholder autoscaler actor."""

    def __init__(self, config: ActorConfig):
        self.config = config
        logger.debug("AutoscalerActor[%s] initialised", config.name)

    def reconcile(self) -> None:
        """Run a reconciliation cycle (no-op for now)."""
        logger.debug("Autoscaler reconcile cycle executed")
