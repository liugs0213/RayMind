"""
PlacementController actor skeleton.
"""

from __future__ import annotations

import logging
from typing import Dict, List

import ray

logger = logging.getLogger(__name__)


@ray.remote
class PlacementControllerActor:
    """Placeholder placement controller."""

    def __init__(self):
        self.placement_groups: Dict[str, dict] = {}
        logger.info("PlacementControllerActor initialised")

    def create_pg(self, name: str, bundles: List[dict], strategy: str = "STRICT_PACK") -> dict:
        self.placement_groups[name] = {"name": name, "bundles": bundles, "strategy": strategy}
        return {"success": True, "pg": self.placement_groups[name]}
