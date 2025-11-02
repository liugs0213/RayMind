"""
Core package bootstrap for the ScheduleMesh runtime.

Re-exports the primary façade classes so callers can simply do::

    from schedulemesh.core import RayScheduler
"""

from __future__ import annotations

from schedulemesh.core.controllers.ray_scheduler import RayScheduler

__all__ = ["RayScheduler"]
