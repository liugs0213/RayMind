"""
Health monitoring utilities for management actors.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class HealthMonitor:
    """Simple placeholder health monitor."""

    def __init__(self, interval: float):
        self.interval = interval
        logger.debug("HealthMonitor created (interval=%s)", interval)

    def start(self) -> None:
        logger.debug("HealthMonitor started")

    def stop(self) -> None:
        logger.debug("HealthMonitor stopped")
