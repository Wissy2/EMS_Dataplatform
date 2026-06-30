"""
parser.py
JSON parsing and message-type detection.
Never raises — all errors are returned as ErrorRecord.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Tuple, Optional

from models import (
    KafkaEnvelope, MqttMeta, TopicMeta, NormalisedRecord,
    ElectricalMeasurements, ProcessVariables, SteamFuelMeasurements,
    WaterAggregate, EnergyAggregate,
    MessageType, ValidationResult, ValidationStatus, ErrorRecord
)

log = logging.getLogger(__name__)

_ELECTRICAL_FIELDS = frozenset([
    "frequency", "voltage_L1N", "voltage_L2N", "voltage_L3N",
    "voltage_L1L2", "voltage_L2L3", "voltage_L3L1",
    "current_L1", "current_L2", "current_L3",
    "thd_voltage", "thd_current", "power_factor",
    "active_power_kW", "reactive_power_kVAR", "apparent_power_kVA",
    "energy_consumption_kWh", "breaker_status", "alarm_trip",
])


def parse_mqtt_topic(raw_topic: str) -> MqttMeta:
    segments = raw_topic.split("/")
    return MqttMeta(
        raw_topic        = raw_topic,
        plant            = segments[0] if len(segments) > 0 else "unknown",
        line             = segments[1] if len(segments) > 1 else "unknown",
        area             = segments[2] if len(segments) > 2 else "unknown",
        equipment_name   = segments[3] if len(segments) > 3 else None,
        measurement_name = segments[4] if len(segments) > 4 else None,
    )


def parse_kafka_topic(kafka_topic: str) -> TopicMeta:
    parts = kafka_topic.split(".")
    return TopicMeta(
        kafka_topic  = kafka_topic,
        line         = parts[1] if len(parts) > 1 else "unknown",
        area         = parts[2] if len(parts) > 2 else "unknown",
        payload_type = parts[3] if len(parts) > 3 else "unknown",
    )


def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _safe_bool(val) -> Optional[bool]:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        return val.lower() in ('true', '1', 't', 'y', 'yes')
    return None


def _parse_timestamp(ts_str: str) -> datetime:
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    return datetime.fromisoformat(ts_str).astimezone(timezone.utc)


def _detect_message_type(payload: dict, kafka_topic: str) -> MessageType:
    if kafka_topic.endswith(".steam_fuel"):
        return MessageType.STEAM_FUEL
    if kafka_topic.endswith("water_consumption"):
        return MessageType.WATER_AGG
    if kafka_topic.endswith("energy_consumption"):
        return MessageType.ENERGY_AGG
    if "measurements" in payload:
        m = payload["measurements"]
        if isinstance(m, dict) and _ELECTRICAL_FIELDS.intersection(m.keys()):
            return MessageType.ELECTRICAL_PM
        return MessageType.PROCESS_VAR
    return MessageType.UNKNOWN


def _parse_electrical(m: dict) -> ElectricalMeasurements:
    return ElectricalMeasurements(
        frequency              = _safe_float(m.get("frequency")),
        voltage_L1N            = _safe_float(m.get("voltage_L1N")),
        voltage_L2N            = _safe_float(m.get("voltage_L2N")),
        voltage_L3N            = _safe_float(m.get("voltage_L3N")),
        voltage_L1L2           = _safe_float(m.get("voltage_L1L2")),
        voltage_L2L3           = _safe_float(m.get("voltage_L2L3")),
        voltage_L3L1           = _safe_float(m.get("voltage_L3L1")),
        current_L1             = _safe_float(m.get("current_L1")),
        current_L2             = _safe_float(m.get("current_L2")),
        current_L3             = _safe_float(m.get("current_L3")),
        thd_voltage            = _safe_float(m.get("thd_voltage")),
        thd_current            = _safe_float(m.get("thd_current")),
        power_factor           = _safe_float(m.get("power_factor")),
        active_power_kW        = _safe_float(m.get("active_power_kW")),
        reactive_power_kVAR    = _safe_float(m.get("reactive_power_kVAR")),
        apparent_power_kVA     = _safe_float(m.get("apparent_power_kVA")),
        energy_consumption_kWh = _safe_float(m.get("energy_consumption_kWh")),
        breaker_status         = _safe_bool(m.get("breaker_status")),
        alarm_trip             = _safe_bool(m.get("alarm_trip")),
    )

def _parse_process_variables(m: dict) -> ProcessVariables:
    return ProcessVariables(
        belt_speed          = _safe_float(m.get("belt_speed")),
        pump_speed          = _safe_float(m.get("pump_speed")),
        agitator_speed      = _safe_float(m.get("agitator_speed")),
        flow                = _safe_float(m.get("flow")),
        instant_flow        = _safe_float(m.get("instant_flow")),
        temperature         = _safe_float(m.get("temperature")),
        volume_totalised_m3 = _safe_float(m.get("volume_totalised_m3")),
        air_pressure        = _safe_float(m.get("air_pressure")),
        air_flow            = _safe_float(m.get("air_flow")),
        speed               = _safe_float(m.get("speed")),
        status              = _safe_bool(m.get("status")),
    )

def _parse_steam_fuel(payload: dict) -> SteamFuelMeasurements:
    s = payload.get("steam", {})
    f = payload.get("fuel", {})
    return SteamFuelMeasurements(
        steam_flow_rate   = _safe_float(s.get("flow_rate")),
        steam_totalizer   = _safe_float(s.get("totalizer")),
        steam_pressure    = _safe_float(s.get("pressure")),
        steam_temperature = _safe_float(s.get("temperature")),
        fuel_flow_rate    = _safe_float(f.get("flow_rate")),
        fuel_totalizer    = _safe_float(f.get("totalizer")),
        fuel_temperature  = _safe_float(f.get("temperature")),
        fuel_pressure     = _safe_float(f.get("pressure")),
    )


def parse_envelope(envelope: KafkaEnvelope) -> Tuple[Optional[NormalisedRecord], Optional[ErrorRecord]]:
    now = datetime.now(timezone.utc)

    try:
        raw_str = envelope.value.decode("utf-8")
        payload = json.loads(raw_str)
    except Exception as e:
        return None, ErrorRecord(
            kafka_topic   = envelope.topic,
            partition     = envelope.partition,
            offset        = envelope.offset,
            error_type    = "JSON_PARSE",
            error_message = str(e),
            raw_payload   = envelope.value.decode("utf-8", errors="replace"),
            processing_ts = now,
        )

    topic_meta = parse_kafka_topic(envelope.topic)
    mqtt_raw = envelope.headers.get("mqtt-topic", "")
    mqtt_meta = parse_mqtt_topic(mqtt_raw) if mqtt_raw else MqttMeta(
        raw_topic="", plant="unknown", line=topic_meta.line,
        area=topic_meta.area, equipment_name=None, measurement_name=None
    )

    try:
        event_time = _parse_timestamp(payload["timestamp"])
    except (KeyError, ValueError) as e:
        return None, ErrorRecord(
            kafka_topic   = envelope.topic,
            partition     = envelope.partition,
            offset        = envelope.offset,
            error_type    = "SCHEMA",
            error_message = f"Bad/missing timestamp: {e}",
            raw_payload   = raw_str,
            processing_ts = now,
        )

    msg_type = _detect_message_type(payload, envelope.topic)

    electrical = process_var = steam_fuel = water_agg = energy_agg = None

    try:
        m = payload.get("measurements", {})
        if msg_type == MessageType.ELECTRICAL_PM:
            electrical = _parse_electrical(m)

        elif msg_type == MessageType.PROCESS_VAR:
            process_var = _parse_process_variables(m)

        elif msg_type == MessageType.STEAM_FUEL:
            steam_fuel = _parse_steam_fuel(payload)

        elif msg_type == MessageType.WATER_AGG:
            water_agg = WaterAggregate(
                line           = payload.get("line", mqtt_meta.line),
                total_water_m3 = _safe_float(payload.get("total_water_m3")),
            )

        elif msg_type == MessageType.ENERGY_AGG:
            energy_agg = EnergyAggregate(
                area             = payload.get("area"),
                line             = payload.get("line"),
                total_energy_kWh = _safe_float(payload.get("total_energy_kWh")),
            )

    except Exception as e:
        return None, ErrorRecord(
            kafka_topic   = envelope.topic,
            partition     = envelope.partition,
            offset        = envelope.offset,
            error_type    = "SCHEMA",
            error_message = f"Measurement parse error: {e}",
            raw_payload   = raw_str,
            processing_ts = now,
        )

    record = NormalisedRecord(
        message_type     = msg_type,
        kafka_topic      = envelope.topic,
        partition        = envelope.partition,
        offset           = envelope.offset,
        event_time       = event_time,
        processing_time  = now,
        plant            = mqtt_meta.plant,
        line             = mqtt_meta.line or topic_meta.line,
        area             = mqtt_meta.area or topic_meta.area,
        equipment_name   = mqtt_meta.equipment_name,
        measurement_name = mqtt_meta.measurement_name,
        device_id        = payload.get("device_id"),
        mqtt_topic       = mqtt_raw,
        validation       = ValidationResult(status=ValidationStatus.VALID),
        electrical       = electrical,
        process_var      = process_var,
        steam_fuel       = steam_fuel,
        water_agg        = water_agg,
        energy_agg       = energy_agg,
        raw_payload      = raw_str,
    )
    return record, None
