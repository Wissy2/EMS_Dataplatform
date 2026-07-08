"""Typed KPI model documentation for the SQL sinks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class HierarchyKey:
    plant_id: int
    line_id: int
    area_id: Optional[int] = None
    tag_id: Optional[str] = None


@dataclass(frozen=True)
class WindowBounds:
    window_start: datetime
    window_end: datetime


@dataclass(frozen=True)
class EquipmentKpi:
    """Equipment KPI row shape written to ems.equipment_kpis."""

    window_start: datetime
    window_end: datetime
    tag_id: str
    energy_delta_kwh: float
    active_power_avg_kw: Optional[float]
    peak_demand_kw: Optional[float]
    runtime_seconds: float
    availability_percent: Optional[float]
