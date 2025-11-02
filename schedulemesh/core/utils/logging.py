"""Logging utilities for ScheduleMesh runtime components."""

from __future__ import annotations

import logging
import sys
from typing import Optional


_HANDLER_NAME = "_schedulemesh_stream_handler"


def configure_runtime_logging(level: int = logging.INFO, formatter: Optional[logging.Formatter] = None) -> None:
    """Ensure that runtime processes emit logs to stdout with a consistent format."""
    root_logger = logging.getLogger()
    if formatter is None:
        formatter = logging.Formatter("[%(levelname)s] %(message)s")

    existing = None
    for handler in root_logger.handlers:
        if getattr(handler, _HANDLER_NAME, False):
            existing = handler
            break

    if existing is None:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)
        setattr(stream_handler, _HANDLER_NAME, True)
        root_logger.addHandler(stream_handler)
    else:
        existing.setFormatter(formatter)
        existing.setLevel(level)

    if root_logger.level > level or root_logger.level == logging.NOTSET:
        root_logger.setLevel(level)


def install_stdout_logger(level: int = logging.INFO, *, include_timestamp: bool = True, prefix: str = "ScheduleMesh") -> None:
    """Attach a stream handler for demos / CLI scripts with optional timestamp."""
    fmt = "%(asctime)s %(levelname)s: %(message)s" if include_timestamp else "%(levelname)s: %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%H:%M:%S")
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger = logging.getLogger(prefix)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level)
