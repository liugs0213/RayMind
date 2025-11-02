"""
Preemption policy definitions and constants.
"""

from __future__ import annotations

import os
from enum import Enum
from typing import Dict, Tuple, TypedDict


class PreemptionThresholds(TypedDict):
    """A dictionary defining preemption thresholds."""
    label_priority_threshold: float
    same_pool_priority_threshold: float


class PreemptionAggressiveness(str, Enum):
    """
    Semantic levels for preemption aggressiveness.

    Using ``str`` as a mixin keeps backward compatibility with existing string
    literals and makes the enum JSON-serialisable.
    """

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


# Defines semantic levels for preemption aggressiveness.
# These levels are translated into concrete numerical thresholds.
PREEMPTION_AGGRESSIVENESS_LEVELS: Dict[PreemptionAggressiveness, PreemptionThresholds] = {
    PreemptionAggressiveness.HIGH: {
        "label_priority_threshold": 0.1,
        "same_pool_priority_threshold": 0.5,
    },
    PreemptionAggressiveness.MEDIUM: {
        "label_priority_threshold": 0.5,
        "same_pool_priority_threshold": 1.0,
    },
    PreemptionAggressiveness.LOW: {
        "label_priority_threshold": 1.0,
        "same_pool_priority_threshold": 3.0,
    },
}


AGGRESSIVENESS_ALIASES: Dict[str, str] = {
    "high_priority": PreemptionAggressiveness.HIGH.value,
    "high-priority": PreemptionAggressiveness.HIGH.value,
    "urgent": PreemptionAggressiveness.HIGH.value,
    "critical": PreemptionAggressiveness.HIGH.value,
    "prod": PreemptionAggressiveness.HIGH.value,
    "medium_priority": PreemptionAggressiveness.MEDIUM.value,
    "medium-priority": PreemptionAggressiveness.MEDIUM.value,
    "normal": PreemptionAggressiveness.MEDIUM.value,
    "default": PreemptionAggressiveness.MEDIUM.value,
    "low_priority": PreemptionAggressiveness.LOW.value,
    "low-priority": PreemptionAggressiveness.LOW.value,
    "background": PreemptionAggressiveness.LOW.value,
    "batch": PreemptionAggressiveness.LOW.value,
}


def resolve_preemption_aggressiveness(
    value: str | PreemptionAggressiveness | None,
) -> Tuple[PreemptionAggressiveness | None, str | None]:
    """
    Resolve user input (enum, string, environment indirection) to aggressiveness.

    Supports the following forms:
        - Enum members (:class:`PreemptionAggressiveness`)
        - String equivalents, case-insensitive (``"high"``, ``"medium"``, ``"low"``)
        - Semantic aliases (``"urgent"``, ``"background"``, etc.)
        - Environment indirection: ``"env:SCHEDULEMESH_PREEMPTION_LEVEL"``

    Returns:
        A tuple of ``(level, hint)`` where ``hint`` describes the resolution source.
        If resolution fails, returns ``(None, error_hint)``.
    """
    if value is None:
        return None, None

    if isinstance(value, PreemptionAggressiveness):
        return value, f"enum:{value.name}"

    if not isinstance(value, str):
        return None, None

    raw = value.strip()
    if not raw:
        return None, None

    # Environment indirection
    if raw.lower().startswith("env:"):
        env_key = raw[4:].strip()
        if not env_key:
            return None, "环境变量键为空"
        env_val = os.getenv(env_key)
        if env_val is None:
            return None, f"环境变量 {env_key} 未设置"
        raw = env_val.strip()
        if not raw:
            return None, f"环境变量 {env_key} 的值为空"
        hint_prefix = f'env:{env_key}="{env_val}"'
    else:
        hint_prefix = None

    alias = AGGRESSIVENESS_ALIASES.get(raw.lower(), raw.lower())
    try:
        level = PreemptionAggressiveness(alias)
    except ValueError:
        return None, hint_prefix or f'value="{raw}"'

    hint = hint_prefix or f'value="{raw}"'
    return level, hint


def normalize_aggressiveness(
    value: str | PreemptionAggressiveness,
) -> PreemptionAggressiveness | None:
    """
    Convert user input into :class:`PreemptionAggressiveness`.

    Args:
        value: Enum instance or case-insensitive string.

    Returns:
        The normalised enum value, or ``None`` if the input is invalid.
    """
    level, _ = resolve_preemption_aggressiveness(value)
    return level

