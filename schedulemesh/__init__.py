"""
ScheduleMesh package skeleton.

This module exposes high-level entry points while keeping heavy dependencies
lazy-imported so packaging tools do not require Ray during metadata builds.
"""

from __future__ import annotations

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version

__all__ = [
    "RayScheduler",
    "ScheduleMesh",
    "SimpleScheduler",
    "SimpleTaskSpec",
    "__version__",
]


try:
    __version__ = version("schedulemesh-core")
except PackageNotFoundError:
    __version__ = "0.0.0"


_LAZY_TARGETS = {
    "RayScheduler": ("schedulemesh.core.controllers", "RayScheduler"),
    "ScheduleMesh": ("schedulemesh.core.controllers", "ScheduleMesh"),
    "SimpleScheduler": ("schedulemesh.simple", "SimpleScheduler"),
    "SimpleTaskSpec": ("schedulemesh.simple", "SimpleTaskSpec"),
}


def __getattr__(name: str):
    """Dynamically load public symbols to avoid importing optional deps early."""
    target = _LAZY_TARGETS.get(name)
    if target is None:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

    module_name, attribute = target
    module = import_module(module_name)
    value = getattr(module, attribute)
    globals()[name] = value  # cache for subsequent lookups
    return value
