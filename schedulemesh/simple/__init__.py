"""
High-level, user-friendly façade for ScheduleMesh.

This package exposes convenience wrappers that streamline common flows such
as creating pools and submitting tasks with automatic preemption handling.
"""

from .client import SimpleScheduler, SimpleTaskSpec  # noqa: F401

__all__ = ["SimpleScheduler", "SimpleTaskSpec"]
