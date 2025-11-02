"""
Shared configuration dataclasses for management actors.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ActorConfig:
    """
    Generic configuration for actors that require health monitoring
    and automatic restarts.
    """

    name: str
    replicas: int = 1
    max_retries: int = 3
    health_check_interval: float = 30.0
    startup_timeout: float = 60.0
    restart_backoff: float = 1.0
    metadata: dict[str, str] = field(default_factory=dict)
    state_path: str | None = None
