"""Configuration helpers for ScheduleMesh.

This module loads optional YAML configuration files to customize runtime
behaviour such as scheduling strategies.  Configuration precedence:

1. Environment variable ``SCHEDULEMESH_CONFIG`` pointing to a YAML file.
2. ``schedulemesh.yaml`` in the current working directory.
3. Built-in defaults bundled with the package (``config/default.yaml``).
"""

from __future__ import annotations

import inspect
import importlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional

import yaml

from schedulemesh.core.scheduling.strategy import (
    LabelRoundRobinStrategy,
    SchedulingStrategy,
    available_strategies as _available_strategies,
    register_strategy,
    unregister_strategy,
)

__all__ = [
    "SchedulingConfig",
    "StrategyConfigEntry",
    "get_scheduling_config",
    "reset_scheduling_config",
]


_ENV_VAR = "SCHEDULEMESH_CONFIG"
_DEFAULT_CONFIG_RESOURCE = "schedulemesh.config.default"


@dataclass
class StrategyConfigEntry:
    name: str
    import_path: Optional[str] = None
    enabled: bool = True


@dataclass
class SchedulingConfig:
    default_strategy: str = "least_busy"
    strategies: List[StrategyConfigEntry] = field(default_factory=list)


_scheduling_config: Optional[SchedulingConfig] = None


def _resolve_config_path() -> Optional[Path]:
    env_path = os.environ.get(_ENV_VAR)
    if env_path:
        candidate = Path(env_path).expanduser()
        if candidate.is_file():
            return candidate

    cwd_file = Path.cwd() / "schedulemesh.yaml"
    if cwd_file.is_file():
        return cwd_file
    return None


def _load_yaml_dict() -> Dict[str, object]:
    path = _resolve_config_path()
    if path is not None:
        with path.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    # Fallback to bundled default configuration
    from importlib import resources

    with resources.open_text("schedulemesh.config", "default.yaml", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _coerce_strategy_entry(raw: Dict[str, object]) -> StrategyConfigEntry:
    name = str(raw.get("name", "")).strip()
    if not name:
        raise ValueError("Strategy entry requires a non-empty 'name'")
    import_path = raw.get("import")
    if import_path is not None:
        import_path = str(import_path).strip()
    enabled = bool(raw.get("enabled", True))
    return StrategyConfigEntry(name=name, import_path=import_path, enabled=enabled)


def _build_scheduling_config(data: Dict[str, object]) -> SchedulingConfig:
    node = data.get("scheduling", {})
    if not isinstance(node, dict):
        raise ValueError("'scheduling' section must be a mapping")

    default_strategy = str(node.get("default_strategy", "least_busy")).strip() or "least_busy"
    raw_entries = node.get("strategies", [])
    entries: List[StrategyConfigEntry] = []

    if isinstance(raw_entries, list):
        for item in raw_entries:
            if not isinstance(item, dict):
                raise ValueError("Each strategy definition must be a mapping")
            entries.append(_coerce_strategy_entry(item))
    elif raw_entries:
        raise ValueError("'strategies' must be a list of mappings")

    return SchedulingConfig(default_strategy=default_strategy, strategies=entries)


def _coerce_strategy_resolver(obj: object) -> Callable[[Optional[Dict[str, str]]], SchedulingStrategy]:
    if inspect.isclass(obj) and issubclass(obj, SchedulingStrategy):  # type: ignore[arg-type]
        if issubclass(obj, LabelRoundRobinStrategy):
            return lambda labels: obj(labels=labels)  # type: ignore[arg-type]
        return lambda _labels: obj()  # type: ignore[arg-type]

    if callable(obj):
        signature = inspect.signature(obj)
        params = list(signature.parameters.values())

        def _call_with_labels(labels: Optional[Dict[str, str]]) -> SchedulingStrategy:
            if not params:
                instance = obj()
            else:
                first = params[0]
                if first.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
                    if first.name == "labels":
                        instance = obj(labels=labels)
                    else:
                        instance = obj(labels)
                else:
                    instance = obj(labels)
            if isinstance(instance, SchedulingStrategy):
                return instance
            if inspect.isclass(instance) and issubclass(instance, SchedulingStrategy):
                # Allow factory returning a class
                cls = instance
                if issubclass(cls, LabelRoundRobinStrategy):
                    return cls(labels=labels)
                return cls()
            raise TypeError("Strategy factory must return SchedulingStrategy or subclass")

        return _call_with_labels

    raise TypeError("Unsupported strategy factory type")


def _apply_scheduling_config(config: SchedulingConfig) -> None:
    for entry in config.strategies:
        if not entry.enabled:
            unregister_strategy(entry.name)
            continue
        resolver = None
        if entry.import_path:
            module_name, sep, attr = entry.import_path.partition(":")
            if not sep:
                raise ValueError(
                    f"Invalid import path '{entry.import_path}'. Expected format 'module:attr'."
                )
            module = importlib.import_module(module_name)
            obj = getattr(module, attr)
            resolver = _coerce_strategy_resolver(obj)
        if resolver is not None:
            register_strategy(entry.name, resolver, replace=True)

    # Validate default strategy is available
    if config.default_strategy not in _available_strategies():
        raise ValueError(
            f"Default strategy '{config.default_strategy}' is not registered. Available: {', '.join(_available_strategies())}"
        )


def get_scheduling_config() -> SchedulingConfig:
    global _scheduling_config
    if _scheduling_config is None:
        raw = _load_yaml_dict()
        config = _build_scheduling_config(raw)
        _apply_scheduling_config(config)
        _scheduling_config = config
    return _scheduling_config


def reset_scheduling_config() -> None:
    """Reset cached scheduling configuration (intended for tests)."""
    global _scheduling_config
    _scheduling_config = None
