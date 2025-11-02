"""
Central resource registry skeleton.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass
class ResourceInfoRecord:
    name: str
    label: str
    resource_type: str
    metadata: Dict[str, str]


class ResourceRegistry:
    """Placeholder registry that can be expanded with metrics aggregation."""

    def __init__(self):
        self.records: Dict[str, ResourceInfoRecord] = {}

    def register(self, record: ResourceInfoRecord) -> None:
        self.records[record.name] = record

    def list_records(self) -> list[ResourceInfoRecord]:
        return list(self.records.values())
