"""
parser.py
JSON → NormalisedRecord.
Produces ErrorRecord on any failure — never raises.
Uses MetadataCache to resolve device_id → tag_id / area_id / line_id / plant_id.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Tuple, Optional

from models import (
    KafkaEnvelope, MqttMeta, NormalisedRecord, ResolvedIds,
    ElectricalMeasurements, ProcessVariables, SteamFuelMeasurements,
    WaterAggregate, EnergyAggregate,
    MessageType, ValidationResult, ValidationStatus, ErrorRecord
)
import config

log = logging.getLogger(__name__)

# Electrical field presence check (payload key names from MQTT, camelCase)
_ELECTRICAL_MARKER = frozenset(["active_power_kW", "energy_consumption_kWh", "frequency"])


def parse_mqtt_topic(raw_topic: str) -> MqttMeta:
    """
    Al_Youssoufia_Plant/Line-1/Extraction/Bucket_Wheel_Excavator/PM1
    → MqttMeta(plant, line, area, equipment_name, measurement_name)
    """
    s = raw_topic.split("/")
    return MqttMeta(
        raw_topic        = raw_topic,
        plant            = s[0] if len(s) > 0 else "unknown",
        line             = s[1] if len(s) > 1 else "unknown",
        area             = s[2] if len(s) > 2 else "unknown",
        equipment_name   = s[3] if len(s) > 3 else None,
        measurement_name = s[4] if len(s) > 4 else None,
    )


def _f(val) -> Optional[float]:
    """Safe float cast."""
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _b(val) -> Optional[bool]:
    """Safe bool cast. Handles int (0/1) and bool."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    try:
        return bool(int(val))
    except (TypeError, ValueError):
        return None


def _parse_ts(ts_str: str) -> datetime:
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    return datetime.fromisoformat(ts_str).astimezone(timezone.utc)


def _detect_type(payload: dict, kafka_topic: str) -> MessageType:
    if kafka_topic.endswith(".steam_fuel"):
        return MessageType.STEAM_FUEL
    if "water_consumption" in kafka_topic:
        return MessageType.WATER_AGG
    if "energy_consumption" in kafka_topic:
        return MessageType.ENERGY_AGG
    m = payload.get("measurements", {})
    if isinstance(m, dict):
        if _ELECTRICAL_MARKER.intersection(m.keys()):
            return MessageType.ELECTRICAL_PM
        if m:
            return MessageType.PROCESS_VAR
    return MessageType.UNKNOWN


def _parse_electrical(m: dict) -> ElectricalMeasurements:
    """
    MQTT payload uses camelCase keys (active_power_kW).
    DB columns use lowercase (active_power_kw).
    Mapping is done explicitly here.
    """
    return ElectricalMeasurements(
        frequency              = _f(m.get("frequency")),
        voltage_l1n            = _f(m.get("voltage_L1N")),
        voltage_l2n            = _f(m.get("voltage_L2N")),
        voltage_l3n            = _f(m.get("voltage_L3N")),
        voltage_l1l2           = _f(m.get("voltage_L1L2")),
        voltage_l2l3           = _f(m.get("voltage_L2L3")),
        voltage_l3l1           = _f(m.get("voltage_L3L1")),
        current_l1             = _f(m.get("current_L1")),
        current_l2             = _f(m.get("current_L2")),
        current_l3             = _f(m.get("current_L3")),
        thd_voltage            = _f(m.get("thd_voltage")),
        thd_current            = _f(m.get("thd_current")),
        power_factor           = _f(m.get("power_factor")),
        active_power_kw        = _f(m.get("active_power_kW")),
        reactive_power_kvar    = _f(m.get("reactive_power_kVAR")),
        apparent_power_kva     = _f(m.get("apparent_power_kVA")),
        energy_consumption_kwh = _f(m.get("energy_consumption_kWh")),
        breaker_status         = _b(m.get("breaker_status")),
        alarm_trip             = _b(m.get("alarm_trip")),
    )


def _parse_process_var(m: dict) -> ProcessVariables:
    """
    Maps every known process variable key.
    Unknown keys in the payload are silently ignored (stored in raw_measurements).
    """
    return ProcessVariables(
        belt_speed          = _f(m.get("belt_speed")),
        pump_speed          = _f(m.get("pump_speed")),
        agitator_speed      = _f(m.get("agitator_speed")),
        flow                = _f(m.get("flow")),
        instant_flow        = _f(m.get("instant_flow")),
        temperature         = _f(m.get("temperature")),
        volume_totalised_m3 = _f(m.get("volume_totalised_m3")),
        air_pressure        = _f(m.get("air_pressure")),
        air_flow            = _f(m.get("air_flow")),
        speed               = _f(m.get("speed")),
        status              = _b(m.get("status")),
    )


def _parse_steam_fuel(payload: dict) -> SteamFuelMeasurements:
    s = payload.get("steam", {})
    f = payload.get("fuel", {})
    return SteamFuelMeasurements(
        steam_flow_rate   = _f(s.get("flow_rate")),
        steam_totalizer   = _f(s.get("totalizer")),
        steam_pressure    = _f(s.get("pressure")),
        steam_temperature = _f(s.get("temperature")),
        fuel_flow_rate    = _f(f.get("flow_rate")),
        fuel_totalizer    = _f(f.get("totalizer")),
        fuel_pressure     = _f(f.get("pressure")),
        fuel_temperature  = _f(f.get("temperature")),
    )


def parse_envelope(
    envelope: KafkaEnvelope,
    cache,                          # MetadataCache — passed in to avoid circular import
) -> Tuple[Optional[NormalisedRecord], Optional[ErrorRecord]]:
    """
    Main parse entry point.
    Returns (NormalisedRecord, None) or (None, ErrorRecord). Never raises.
    """
    now = datetime.now(timezone.utc)

    # ── JSON ──────────────────────────────────────────────────────────────────
    try:
        raw_str = envelope.value.decode("utf-8")
        payload = json.loads(raw_str)
    except Exception as exc:
        return None, ErrorRecord(
            kafka_topic   = envelope.topic,
            partition     = envelope.partition,
            offset        = envelope.offset,
            error_type    = "JSON_PARSE",
            error_message = str(exc),
            raw_payload   = envelope.value.decode("utf-8", errors="replace"),
            processing_ts = now,
        )

    # ── MQTT topic metadata ───────────────────────────────────────────────────
    mqtt_raw   = envelope.headers.get("mqtt-topic", "")
    mqtt_meta  = parse_mqtt_topic(mqtt_raw) if mqtt_raw else MqttMeta(
        raw_topic="", plant="", line="", area="", equipment_name=None, measurement_name=None
    )

    # ── Timestamp ─────────────────────────────────────────────────────────────
    try:
        event_time = _parse_ts(payload["timestamp"])
    except (KeyError, ValueError) as exc:
        return None, ErrorRecord(
            kafka_topic   = envelope.topic,
            partition     = envelope.partition,
            offset        = envelope.offset,
            error_type    = "SCHEMA",
            error_message = f"Bad/missing timestamp: {exc}",
            raw_payload   = raw_str,
            processing_ts = now,
            mqtt_topic    = mqtt_raw,
        )

    # ── Message type ──────────────────────────────────────────────────────────
    msg_type = _detect_type(payload, envelope.topic)

    # ── Resolve FK IDs from metadata cache ────────────────────────────────────
    device_id = payload.get("device_id")
    ids = ResolvedIds(plant_id=None, line_id=None, area_id=None, tag_id=None)

    if device_id:
        resolved = cache.resolve_device(device_id)
        if resolved:
            ids = resolved
        else:
            # Device not yet registered in ems.equipment
            # Still store the record in raw_measurements — don't discard
            log.warning("Unregistered device_id=%s topic=%s", device_id, envelope.topic)

    elif msg_type == MessageType.WATER_AGG:
        line_str = payload.get("line", mqtt_meta.line)
        ids.line_id = cache.resolve_line(line_str)

    elif msg_type == MessageType.ENERGY_AGG:
        line_str = payload.get("line", mqtt_meta.line)
        area_str = payload.get("area")
        ids.line_id = cache.resolve_line(line_str) if line_str else None
        ids.area_id = cache.resolve_area(area_str) if area_str else None

    elif msg_type == MessageType.STEAM_FUEL:
        line_str = mqtt_meta.line or ""
        ids.line_id = cache.resolve_line(line_str)

    # ── Typed measurements ────────────────────────────────────────────────────
    electrical = process_var = steam_fuel = water_agg = energy_agg = None
    try:
        m = payload.get("measurements", {})
        if msg_type == MessageType.ELECTRICAL_PM:
            electrical = _parse_electrical(m)
        elif msg_type == MessageType.PROCESS_VAR:
            process_var = _parse_process_var(m)
        elif msg_type == MessageType.STEAM_FUEL:
            steam_fuel = _parse_steam_fuel(payload)
        elif msg_type == MessageType.WATER_AGG:
            water_agg = WaterAggregate(
                total_water_m3=_f(payload.get("total_water_m3"))
            )
        elif msg_type == MessageType.ENERGY_AGG:
            energy_agg = EnergyAggregate(
                area_id          = ids.area_id,
                total_energy_kwh = _f(payload.get("total_energy_kWh")),
            )
    except Exception as exc:
        return None, ErrorRecord(
            kafka_topic   = envelope.topic,
            partition     = envelope.partition,
            offset        = envelope.offset,
            error_type    = "SCHEMA",
            error_message = f"Measurement parse error: {exc}",
            raw_payload   = raw_str,
            processing_ts = now,
            mqtt_topic    = mqtt_raw,
        )

    # ── Assemble ──────────────────────────────────────────────────────────────
    record = NormalisedRecord(
        message_type    = msg_type,
        kafka_topic     = envelope.topic,
        partition       = envelope.partition,
        offset          = envelope.offset,
        event_time      = event_time,
        processing_time = now,
        mqtt_topic      = mqtt_raw,
        plant           = mqtt_meta.plant,
        line            = mqtt_meta.line,
        area            = mqtt_meta.area,
        equipment_name  = mqtt_meta.equipment_name,
        device_id       = device_id,
        ids             = ids,
        validation      = ValidationResult(),   # populated by validator next
        electrical      = electrical,
        process_var     = process_var,
        steam_fuel      = steam_fuel,
        water_agg       = water_agg,
        energy_agg      = energy_agg,
        raw_payload     = raw_str,
    )
    return record, None
