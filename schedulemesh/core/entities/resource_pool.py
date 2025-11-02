"""
Resource pool entity definitions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class ResourceSpec:
    """Represents a bundle of compute resources."""

    cpu: float = 0.0
    memory: float = 0.0
    gpu: float = 0.0
    custom: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, float]:
        data: Dict[str, float] = {"cpu": self.cpu, "memory": self.memory, "gpu": self.gpu}
        if self.custom:
            data["custom"] = dict(self.custom)
        return data

    @classmethod
    def from_dict(cls, values: Dict[str, float]) -> "ResourceSpec":
        """
        从字典创建ResourceSpec对象
        
        Args:
            values: 资源字典
            
        Returns:
            ResourceSpec对象
            
        Raises:
            ValueError: 如果资源值无效（负数或无法转换）
        """
        try:
            cpu = float(values.get("cpu", 0.0))
            memory = float(values.get("memory", 0.0))
            gpu = float(values.get("gpu", 0.0))
            
            # 验证非负
            if cpu < 0 or memory < 0 or gpu < 0:
                raise ValueError(f"Resource values must be non-negative: cpu={cpu}, memory={memory}, gpu={gpu}")
            
            custom_values: Dict[str, float] = {}
            raw_custom = values.get("custom")
            if isinstance(raw_custom, dict):
                for key, val in raw_custom.items():
                    custom_val = float(val)
                    if custom_val < 0:
                        raise ValueError(f"Custom resource '{key}' must be non-negative, got {custom_val}")
                    custom_values[key] = custom_val

            for key, value in values.items():
                if key not in {"cpu", "memory", "gpu", "custom"}:
                    custom_val = float(value)
                    if custom_val < 0:
                        raise ValueError(f"Custom resource '{key}' must be non-negative, got {custom_val}")
                    custom_values[key] = custom_val

            return cls(cpu=cpu, memory=memory, gpu=gpu, custom=custom_values)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid resource specification: {e}") from e

    @classmethod
    def from_ray_resources(cls, resources: Dict[str, float]) -> "ResourceSpec":
        cpu = resources.get("CPU", 0.0)
        memory = resources.get("memory", 0.0)
        gpu = resources.get("GPU", 0.0)

        custom_values: Dict[str, float] = {}
        for key, value in resources.items():
            if key not in {"CPU", "GPU", "memory"}:
                custom_values[key] = float(value)

        return cls(cpu=cpu, memory=memory, gpu=gpu, custom=custom_values)

    def multiply(self, factor: float) -> "ResourceSpec":
        """
        将资源规格乘以一个因子
        
        Args:
            factor: 乘数因子（应为非负数）
            
        Returns:
            新的ResourceSpec对象
            
        Raises:
            ValueError: 如果factor为负数
        """
        if factor < 0:
            raise ValueError(f"Resource multiply factor must be non-negative, got {factor}")
        multiplied_custom = {key: val * factor for key, val in self.custom.items()}
        return ResourceSpec(
            cpu=self.cpu * factor,
            memory=self.memory * factor,
            gpu=self.gpu * factor,
            custom=multiplied_custom,
        )

    def copy(self) -> "ResourceSpec":
        return ResourceSpec(cpu=self.cpu, memory=self.memory, gpu=self.gpu, custom=dict(self.custom))

    def add_inplace(self, other: "ResourceSpec") -> None:
        self.cpu += other.cpu
        self.memory += other.memory
        self.gpu += other.gpu
        for key, value in other.custom.items():
            self.custom[key] = self.custom.get(key, 0.0) + value

    def subtract_inplace(self, other: "ResourceSpec") -> None:
        if other.cpu > self.cpu or other.memory > self.memory or other.gpu > self.gpu:
            raise ValueError("Resource subtraction would go negative")
        self.cpu -= other.cpu
        self.memory -= other.memory
        self.gpu -= other.gpu
        for key, value in other.custom.items():
            current = self.custom.get(key, 0.0)
            if value > current:
                raise ValueError("Resource subtraction would go negative")
            remaining = current - value
            if remaining == 0:
                self.custom.pop(key, None)
            else:
                self.custom[key] = remaining

    def __add__(self, other: "ResourceSpec") -> "ResourceSpec":
        result = self.copy()
        result.add_inplace(other)
        return result

    def __sub__(self, other: "ResourceSpec") -> "ResourceSpec":
        result = self.copy()
        result.subtract_inplace(other)
        return result

    def has_enough(self, other: "ResourceSpec") -> bool:
        if self.cpu < other.cpu or self.memory < other.memory or self.gpu < other.gpu:
            return False
        for key, value in other.custom.items():
            if self.custom.get(key, 0.0) < value:
                return False
        return True

    def __repr__(self) -> str:
        custom_repr = ", ".join(f"{k}={v:.2f}" for k, v in sorted(self.custom.items()))
        return (
            f"ResourceSpec(cpu={self.cpu:.2f}, memory={self.memory:.2f}, gpu={self.gpu:.2f}"
            + (f", custom={{ {custom_repr} }}" if custom_repr else "")
            + ")"
        )

    def to_ray_options(self) -> Dict[str, float]:
        options: Dict[str, float] = {}
        if self.cpu > 0:
            options["num_cpus"] = self.cpu
        if self.gpu > 0:
            options["num_gpus"] = self.gpu
        if self.memory > 0:
            options["memory"] = self.memory * 1024 * 1024  # interpret as MB
        if self.custom:
            options["resources"] = dict(self.custom)
        return options


@dataclass
class PoolQuota:
    """Quota settings for a resource pool."""

    capacity: ResourceSpec

    def to_dict(self) -> Dict[str, float]:
        return self.capacity.to_dict()


@dataclass
class PoolLedger:
    """Track allocation state within a pool at resource granularity."""

    quota: PoolQuota
    used: ResourceSpec = field(default_factory=ResourceSpec)
    reserved: ResourceSpec = field(default_factory=ResourceSpec)

    def available(self) -> ResourceSpec:
        return self.quota.capacity - self.used - self.reserved

    def reserve(self, resources: ResourceSpec) -> None:
        if not self.available().has_enough(resources):
            raise ValueError("Insufficient pool capacity for requested resources")
        self.reserved.add_inplace(resources)

    def commit(self, resources: ResourceSpec) -> None:
        if not self.reserved.has_enough(resources):
            raise ValueError("Cannot commit more resources than reserved")
        self.reserved.subtract_inplace(resources)
        self.used.add_inplace(resources)

    def release(self, resources: ResourceSpec) -> None:
        if not self.used.has_enough(resources):
            raise ValueError("Cannot release more resources than currently used")
        self.used.subtract_inplace(resources)

    def cancel(self, resources: ResourceSpec) -> None:
        if not self.reserved.has_enough(resources):
            raise ValueError("Cannot cancel reservation that does not exist")
        self.reserved.subtract_inplace(resources)


class ResourcePool:
    """Simple container for resource pool metadata."""

    def __init__(
        self,
        name: str,
        labels: Dict[str, str],
        capacity: Dict[str, float],
        *,
        default_agent_resources: Optional[Dict[str, float]] = None,
        target_agents: int = 0,
        placement_strategy: str = "STRICT_PACK",
    ):
        self.name = name
        self.labels = labels
        self.capacity = ResourceSpec.from_dict(capacity)
        self.default_agent_resources = (
            ResourceSpec.from_dict(default_agent_resources) if default_agent_resources else None
        )
        self.target_agents = target_agents
        self.placement_strategy = placement_strategy
        self.ledger = PoolLedger(
            quota=PoolQuota(capacity=self.capacity.copy()),
        )
        self.metadata: Dict[str, str] = {}

    def to_dict(self) -> Dict[str, object]:
        """Serialize pool metadata for transport."""
        return {
            "name": self.name,
            "labels": self.labels,
            "capacity": self.capacity.to_dict(),
            "target_agents": self.target_agents,
            "placement_strategy": self.placement_strategy,
            "used_resources": self.ledger.used.to_dict(),
            "reserved_resources": self.ledger.reserved.to_dict(),
            "available_resources": self.ledger.available().to_dict(),
            "default_agent_resources": self.default_agent_resources.to_dict()
            if self.default_agent_resources
            else None,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, object]) -> "ResourcePool":
        name = str(payload.get("name"))
        labels = dict(payload.get("labels") or {})
        capacity = payload.get("capacity", {})
        default_agent_resources = payload.get("default_agent_resources")
        target_agents = int(payload.get("target_agents", 0) or 0)
        placement_strategy = str(payload.get("placement_strategy") or "STRICT_PACK")

        pool = cls(
            name=name,
            labels=labels,
            capacity=capacity,
            default_agent_resources=default_agent_resources,
            target_agents=target_agents,
            placement_strategy=placement_strategy,
        )

        used_resources = payload.get("used_resources", {})
        reserved_resources = payload.get("reserved_resources", {})
        pool.ledger.used = ResourceSpec.from_dict(used_resources)
        pool.ledger.reserved = ResourceSpec.from_dict(reserved_resources)
        return pool
