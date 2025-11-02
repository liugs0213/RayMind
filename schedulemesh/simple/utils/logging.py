"""Helper logging configuration for simple demos."""

from __future__ import annotations

import logging


DEMO_LOGGER_NAME = "SimpleDemo"


def configure_demo_logging(*, include_timestamp: bool = True) -> logging.Logger:
    fmt = "%(asctime)s [%(name)s] %(levelname)s %(message)s" if include_timestamp else "[%(name)s] %(levelname)s %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(DEMO_LOGGER_NAME)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def demote_ray_logging(level: int = logging.ERROR) -> None:
    for name in ("ray", "aioredis", "aiogrpc"):
        logging.getLogger(name).setLevel(level)


def suppress_actor_prefix() -> None:
    logging.getLogger("ray.ray_logger").setLevel(logging.ERROR)
