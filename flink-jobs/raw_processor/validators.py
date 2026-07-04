"""
validators.py
Validation rules applied after parsing.
Key rule: NEVER drop a record. Flag it, store it, let downstream decide.
"""

import logging
from datetime import datetime, timezone

from models import NormalisedRecord, MessageType, ValidationResult, ValidationStatus
import config

log = logging.getLogger(__name__)


def _flag(result: ValidationResult, flag: str, make_invalid: bool = True) -> None:
    result.flags.append(flag)
    if make_invalid:
        result.status = ValidationStatus.INVALID


def validate_electrical(record: NormalisedRecord) -> None:
    """Validate electrical PM payload fields."""
    e = record.electrical
    if e is None:
        _flag(record.validation, "missing_electrical_measurements")
        return

    # Required presence
    if e.active_power_kw is None:
        _flag(record.validation, "missing_active_power")

    if e.energy_consumption_kwh is None:
        _flag(record.validation, "missing_energy_kwh")

    # Physical range checks — flag as INVALID but still store
    if e.active_power_kw is not None and e.active_power_kw < 0:
        _flag(record.validation, "negative_active_power")

    if e.reactive_power_kvar is not None and e.reactive_power_kvar < 0:
        _flag(record.validation, "negative_reactive_power", make_invalid=False)  # warning only

    if e.apparent_power_kva is not None and e.apparent_power_kva < 0:
        _flag(record.validation, "negative_apparent_power")

    if e.energy_consumption_kwh is not None and e.energy_consumption_kwh < 0:
        _flag(record.validation, "negative_energy_kwh")

    # Frequency range
    if e.frequency is not None:
        if e.frequency > config.MAX_FREQUENCY_HZ:
            _flag(record.validation, f"frequency_above_{config.MAX_FREQUENCY_HZ}Hz")
        if e.frequency < 0:
            _flag(record.validation, "negative_frequency")

    # Power factor must be in [-1, 1]
    if e.power_factor is not None and not (-1.0 <= e.power_factor <= 1.0):
        _flag(record.validation, "power_factor_out_of_range")

    # THD sanity (>100% is physically impossible for voltage)
    if e.thd_voltage is not None and e.thd_voltage > 100:
        _flag(record.validation, "thd_voltage_above_100pct", make_invalid=False)

    if e.thd_current is not None and e.thd_current > 100:
        _flag(record.validation, "thd_current_above_100pct", make_invalid=False)

    # Voltage sanity (> 1000V L-N is unusual for industrial 3-phase at this scale)
    for vfield, label in [
        (e.voltage_l1n, "V_L1N"), (e.voltage_l2n, "V_L2N"), (e.voltage_l3n, "V_L3N"),
    ]:
        if vfield is not None and vfield > 1000:
            _flag(record.validation, f"{label}_above_1000V", make_invalid=False)

    # Alarm trip — must be 0 or 1
    if e.alarm_trip is not None and e.alarm_trip not in (0, 1):
        _flag(record.validation, "alarm_trip_invalid_value", make_invalid=False)

    # Breaker status — must be 0 or 1
    if e.breaker_status is not None and e.breaker_status not in (0, 1):
        _flag(record.validation, "breaker_status_invalid_value", make_invalid=False)


def validate_steam_fuel(record: NormalisedRecord) -> None:
    s = record.steam_fuel
    if s is None:
        _flag(record.validation, "missing_steam_fuel_measurements")
        return

    if s.steam_flow_rate is not None and s.steam_flow_rate < 0:
        _flag(record.validation, "negative_steam_flow")

    if s.fuel_flow_rate is not None and s.fuel_flow_rate < 0:
        _flag(record.validation, "negative_fuel_flow")

    if s.steam_pressure is not None and s.steam_pressure < 0:
        _flag(record.validation, "negative_steam_pressure")

    if s.fuel_pressure is not None and s.fuel_pressure < 0:
        _flag(record.validation, "negative_fuel_pressure")


def validate_water_agg(record: NormalisedRecord) -> None:
    w = record.water_agg
    if w is None:
        _flag(record.validation, "missing_water_aggregate")
        return

    if w.total_water_m3 is None:
        _flag(record.validation, "missing_total_water_m3")

    if w.total_water_m3 is not None and w.total_water_m3 < 0:
        _flag(record.validation, "negative_water_consumption")


def validate_energy_agg(record: NormalisedRecord) -> None:
    e = record.energy_agg
    if e is None:
        _flag(record.validation, "missing_energy_aggregate")
        return

    if e.total_energy_kwh is None:
        _flag(record.validation, "missing_total_energy_kwh")

    if e.total_energy_kwh is not None and e.total_energy_kwh < 0:
        _flag(record.validation, "negative_energy_consumption")


def validate_timestamp(record: NormalisedRecord) -> None:
    """Cross-cutting: timestamp sanity regardless of message type."""
    now = datetime.now(timezone.utc)
    delta_s = (record.event_time - now).total_seconds()

    if delta_s > config.MAX_FUTURE_TIMESTAMP_S:
        _flag(record.validation, f"timestamp_future_by_{int(delta_s)}s")

    # More than 24h old is suspicious — flag but don't invalidate
    if delta_s < -86400:
        _flag(record.validation, "timestamp_older_than_24h", make_invalid=False)


def validate_record(record: NormalisedRecord) -> NormalisedRecord:
    """
    Entry point. Apply all relevant validators to record.
    Mutates record.validation in place, then returns the record.
    Always returns the record — never None.
    """
    # Timestamp check applies to all types
    validate_timestamp(record)

    if record.message_type == MessageType.ELECTRICAL_PM:
        validate_electrical(record)

    elif record.message_type == MessageType.STEAM_FUEL:
        validate_steam_fuel(record)

    elif record.message_type == MessageType.WATER_AGG:
        validate_water_agg(record)

    elif record.message_type == MessageType.ENERGY_AGG:
        validate_energy_agg(record)

    # PROCESS_VAR: no strict schema — all values accepted
    # UNKNOWN: flagged invalid by default
    elif record.message_type == MessageType.UNKNOWN:
        _flag(record.validation, "unrecognised_message_type")

    if record.validation.flags:
        log.debug(
            "Validation flags for device=%s offset=%d: %s",
            record.device_id, record.offset, record.validation.flags
        )

    return record
