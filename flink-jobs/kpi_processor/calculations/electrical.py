"""Electrical KPI formulas.

Source labels:
- Excel/business: formulas listed in the user's company workbook request.
- Additional industrial KPI: formulas commonly used for industrial monitoring.
"""

from __future__ import annotations

import math
from statistics import mean, pstdev
from typing import Iterable, Optional, Sequence


def safe_div(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def apparent_power_kva(active_power_kw: Optional[float], power_factor: Optional[float]) -> Optional[float]:
    """Excel/business: S = abs(P / cos_phi)."""
    value = safe_div(active_power_kw, power_factor)
    return abs(value) if value is not None else None


def reactive_power_kvar(active_power_kw: Optional[float], apparent_power: Optional[float]) -> Optional[float]:
    """Excel/business: Q = sqrt(S^2 - P^2)."""
    if active_power_kw is None or apparent_power is None:
        return None
    return math.sqrt(max(apparent_power * apparent_power - active_power_kw * active_power_kw, 0.0))


def cumulative_delta(current: Optional[float], previous: Optional[float]) -> float:
    """Delta for cumulative meters; reset/rollover produces the current reading."""
    if current is None:
        return 0.0
    if previous is None:
        return 0.0
    delta = current - previous
    return delta if delta >= 0 else current


def avg(values: Iterable[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    return mean(cleaned) if cleaned else None


def coefficient_of_variation(values: Sequence[Optional[float]]) -> Optional[float]:
    """Additional industrial KPI: population stddev / mean for voltage stability."""
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    base = mean(cleaned)
    if base == 0:
        return None
    return pstdev(cleaned) / base


def phase_unbalance_percent(values: Sequence[Optional[float]]) -> Optional[float]:
    """Additional industrial KPI: max phase deviation from average as a percent."""
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    base = mean(cleaned)
    if base == 0:
        return None
    return max(abs(v - base) for v in cleaned) / base * 100.0


def load_factor(avg_power_kw: Optional[float], peak_demand_kw: Optional[float]) -> Optional[float]:
    """Additional industrial KPI: average power / peak demand."""
    return safe_div(avg_power_kw, peak_demand_kw)


def quality_flag(
    avg_power_factor: Optional[float],
    avg_thd_voltage: Optional[float],
    voltage_cv: Optional[float],
    thresholds: dict[str, float],
) -> str:
    if avg_power_factor is not None and avg_power_factor < thresholds.get("min_power_factor", 0.9):
        return "POOR_POWER_FACTOR"
    if avg_thd_voltage is not None and avg_thd_voltage > thresholds.get("max_thd_voltage", 5.0):
        return "HIGH_THD"
    if voltage_cv is not None and voltage_cv > thresholds.get("max_voltage_cv", 0.02):
        return "UNSTABLE_VOLTAGE"
    return "OK"

