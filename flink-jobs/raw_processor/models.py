"""
models.py
Typed dataclasses matching the ems.* TimescaleDB schema exactly.
Key difference from v1: line_id / area_id are INTEGER FKs, while
equipment is now keyed by tag_id (VARCHAR, e.g. '310L2-AG-01').
The metadata lookup step resolves them.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum


class MessageType(str, Enum):
    ELECTRICAL_PM = "ELECTRICAL_PM"
    PROCESS_VAR   = "PROCESS_VAR"
    STEAM_FUEL    = "STEAM_FUEL"
    WATER_AGG     = "WATER_AGG"
    ENERGY_AGG    = "ENERGY_AGG"
    UNKNOWN       = "UNKNOWN"


class ValidationStatus(str, Enum):
    VALID   = "VALID"
    INVALID = "INVALID"
    WARNING = "WARNING"


# ── Kafka envelope (pre-parse) ────────────────────────────────────────────────
@dataclass
class KafkaEnvelope:
    topic:      str
    partition:  int
    offset:     int
    kafka_ts:   int
    key:        Optional[str]
    value:      bytes
    headers:    Dict[str, str]


# ── MQTT path segments (from mqtt-topic header) ───────────────────────────────
@dataclass
class MqttMeta:
    raw_topic:        str
    plant:            str       # Al_Youssoufia_Plant
    line:             str       # Line-1
    area:             str       # Extraction
    equipment_name:   Optional[str]   # Bucket_Wheel_Excavator
    measurement_name: Optional[str]   # PM1 | Process_Variables


# ── Resolved FK IDs (from ems metadata tables) ────────────────────────────────
@dataclass
class ResolvedIds:
    """
    Looked up once from the ems.* metadata tables at job startup
    and cached in a dict keyed by device_id.
    """
    plant_id:     Optional[int]
    line_id:      Optional[int]
    area_id:      Optional[int]
    tag_id:       Optional[str]   # None for aggregate messages (water/energy/steam)


# ── Validation ────────────────────────────────────────────────────────────────
@dataclass
class ValidationResult:
    status: ValidationStatus = ValidationStatus.VALID
    flags:  list = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return self.status != ValidationStatus.INVALID


# ── Typed measurement structs (match DB columns exactly) ─────────────────────

@dataclass
class ElectricalMeasurements:
    frequency:              Optional[float] = None
    voltage_l1n:            Optional[float] = None
    voltage_l2n:            Optional[float] = None
    voltage_l3n:            Optional[float] = None
    voltage_l1l2:           Optional[float] = None
    voltage_l2l3:           Optional[float] = None
    voltage_l3l1:           Optional[float] = None
    current_l1:             Optional[float] = None
    current_l2:             Optional[float] = None
    current_l3:             Optional[float] = None
    thd_voltage:            Optional[float] = None
    thd_current:            Optional[float] = None
    power_factor:           Optional[float] = None
    active_power_kw:        Optional[float] = None
    reactive_power_kvar:    Optional[float] = None
    apparent_power_kva:     Optional[float] = None
    energy_consumption_kwh: Optional[float] = None
    breaker_status:         Optional[bool]  = None   # DB: BOOLEAN
    alarm_trip:             Optional[bool]  = None   # DB: BOOLEAN


@dataclass
class ProcessVariables:
    """
    Explicit typed columns matching ems.process_variables exactly.
    All nullable — each device only populates its relevant fields.
    """
    belt_speed:          Optional[float] = None
    pump_speed:          Optional[float] = None
    agitator_speed:      Optional[float] = None
    flow:                Optional[float] = None
    instant_flow:        Optional[float] = None
    temperature:         Optional[float] = None
    volume_totalised_m3: Optional[float] = None
    air_pressure:        Optional[float] = None
    air_flow:            Optional[float] = None
    speed:               Optional[float] = None
    status:              Optional[bool]  = None   # DB: BOOLEAN


@dataclass
class SteamFuelMeasurements:
    steam_flow_rate:   Optional[float] = None
    steam_totalizer:   Optional[float] = None
    steam_pressure:    Optional[float] = None
    steam_temperature: Optional[float] = None
    fuel_flow_rate:    Optional[float] = None
    fuel_totalizer:    Optional[float] = None
    fuel_pressure:     Optional[float] = None
    fuel_temperature:  Optional[float] = None


@dataclass
class WaterAggregate:
    total_water_m3: Optional[float] = None


@dataclass
class EnergyAggregate:
    area_id:          Optional[int]   = None   # None = line total
    total_energy_kwh: Optional[float] = None


# ── Normalised record (fully resolved, ready to insert) ───────────────────────
@dataclass
class NormalisedRecord:
    # Routing
    message_type:    MessageType
    kafka_topic:     str
    partition:       int
    offset:          int

    # Timestamps
    event_time:      datetime
    processing_time: datetime

    # Raw MQTT metadata
    mqtt_topic:      str
    plant:           str
    line:            str
    area:            str
    equipment_name:  Optional[str]
    device_id:       Optional[str]

    # Resolved integer FKs (from ems metadata tables)
    ids: ResolvedIds

    # Validation
    validation: ValidationResult

    # Typed measurements (only one is populated per record)
    electrical:  Optional[ElectricalMeasurements]  = None
    process_var: Optional[ProcessVariables]         = None
    steam_fuel:  Optional[SteamFuelMeasurements]    = None
    water_agg:   Optional[WaterAggregate]           = None
    energy_agg:  Optional[EnergyAggregate]          = None

    # Original payload — immutable truth
    raw_payload: str = ""


# ── DLQ error record ──────────────────────────────────────────────────────────
@dataclass
class ErrorRecord:
    kafka_topic:   str
    partition:     int
    offset:        int
    error_type:    str    # JSON_PARSE | SCHEMA | LOOKUP | DB_WRITE
    error_message: str
    raw_payload:   str
    processing_ts: datetime
    mqtt_topic:    str = ""
