"""
Shared pytest fixtures.
"""

from __future__ import annotations

import logging
import uuid

import pytest
import ray

from schedulemesh.core.controllers.ray_scheduler import RayScheduler

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s", force=True)
logging.getLogger("schedulemesh").setLevel(logging.DEBUG)


@pytest.fixture
def ray_runtime():
    """Spin up a local Ray runtime for tests and mirror worker logs to the driver."""
    try:
        ray.init(
            ignore_reinit_error=True,
            local_mode=True,
            logging_level=logging.INFO,
        )
    except PermissionError as exc:
        pytest.skip(f"Ray init requires system permissions not available in this environment: {exc}")
    except Exception as exc:  # pragma: no cover - defensive guard for restricted sandboxes
        if "Operation not permitted" in str(exc):
            pytest.skip(f"Ray init skipped due to restricted environment: {exc}")
        raise
    try:
        yield
    finally:
        ray.shutdown()


@pytest.fixture
def scheduler(ray_runtime):
    """Provide an isolated RayScheduler instance per test."""
    name = f"test-scheduler-{uuid.uuid4().hex[:8]}"
    instance = RayScheduler(name=name)
    try:
        yield instance
    finally:
        instance.shutdown()
