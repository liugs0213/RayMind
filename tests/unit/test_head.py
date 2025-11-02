"""
Unit tests for the ScheduleMesh head helper.
"""

from __future__ import annotations

from schedulemesh.core.actors.head import ScheduleMeshHead


def test_head_start_stop(ray_runtime):
    head = ScheduleMeshHead(name="test-schedulemesh-supervisor")

    assert head.start() is True
    assert head._actor is not None  # pylint: disable=protected-access

    assert head.stop() is True
    assert head._actor is None  # pylint: disable=protected-access
