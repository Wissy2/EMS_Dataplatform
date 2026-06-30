"""
models.py
Typed dataclasses for every message type flowing through the pipeline.
Using dataclasses (not Pydantic) to stay dependency-light inside Flink.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum


class MessageType(str, Enum):
    ELECTRICAL_PM   = "ELECTRICAL_PM"
    PROCESS_VAR     = "PROCESS_VAR"
    STEAM_FUEL      = "STEAM_FUEL"
    WATER_AGG       = "WATER_AGG"
    ENERGY_AGG      = "ENERGY_AGG"
    UNKNOWN         = "UNKNOWN"


class ValidationStatus(str, Enum):
    VALID       = "VALID"
    INVALID     = "INVALID"
    WARNING     = "WARNING"


# ── Kafka envelope ─────────────────────────────────────────────────────────────
@dataclass
class KafkaEnvelope:
    topic:      str
    partition:  int
    offset:     int
    kafka_ts:   int
    key:        Optional[str]
    value:      bytes
    headers:    Dict[str, str]


# ── MQTT path metadata ─────────────────────────────────────────────────────────
@dataclass
class MqttMeta:
    raw_topic:        str
    plant:            str
    line:             str
    area:             str
    equipment_name:   Optional[str]
    measurement_name: Optional[str]


# ── Kafka topic metadata ───────────────────────────────────────────────────────
@dataclass
class TopicMeta:
    kafka_topic:  str
    line:         str
    area:         str
    payload_type: str


# ── Validation result ─────────────────────────────────────────────────────────
@dataclass
class ValidationResult:
    status:   ValidationStatus
    flags:    list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return self.status != ValidationStatus.INVALID


# ── Typed measurement structs ──────────────────────────────────────────────────
@dataclass
class ElectricalMeasurements:
    frequency:              Optional[float]
    voltage_L1N:            Optional[float]
    voltage_L2N:            Optional[float]
    voltage_L3N:            Optional[float]
    voltage_L1L2:           Optional[float]
    voltage_L2L3:           Optional[float]
    voltage_L3L1:           Optional[float]
    current_L1:             Optional[float]
    current_L2:             Optional[float]
    current_L3:             Optional[float]
    thd_voltage:            Optional[float]
    thd_current:            Optional[float]
    power_factor:           Optional[float]
    active_power_kW:        Optional[float]
    reactive_power_kVAR:    Optional[float]
    apparent_power_kVA:     Optional[float]
    energy_consumption_kWh: Optional[float]
    breaker_status:         Optional[bool]
    alarm_trip:             Optional[bool]


@dataclass
class ProcessVariables:
    """Explicitly defined to match ems.process_variables schema"""
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
    status:              Optional[bool] = None


@dataclass
class SteamFuelMeasurements:
    steam_flow_rate:    Optional[float]
    steam_totalizer:    Optional[float]
    steam_pressure:     Optional[float]
    steam_temperature:  Optional[float]
    fuel_flow_rate:     Optional[float]
    fuel_totalizer:     Optional[float]
    fuel_temperature:   Optional[float]
    fuel_pressure:      Optional[float]


@dataclass
class WaterAggregate:
    line:            str
    total_water_m3:  Optional[float]


@dataclass
class EnergyAggregate:
    area:              Optional[str]
    line:              Optional[str]
    total_energy_kWh:  Optional[float]


# ── Normalised record (post-parse, pre-insert) ────────────────────────────────
@dataclass
class NormalisedRecord:
    # Routing / identity
    message_type:         MessageType
    kafka_topic:          str
    partition:            int
    offset:               int

    # Timestamps
    event_time:           datetime
    processing_time:      datetime

    # Source identifiers (from MQTT path / JSON)
    plant:                str
    line:                 str
    area:                 str
    equipment_name:       Optional[str]
    measurement_name:     Optional[str]
    device_id:            Optional[str]
    mqtt_topic:           Optional[str]     # Retained for raw_measurements
    
    # Validation (Moved above the defaults to fix the TypeError)
    validation:           ValidationResult

    # Resolved Database IDs (Filled by Enricher)
    plant_id:             Optional[int] = None
    line_id:              Optional[int] = None
    area_id:              Optional[int] = None
    equipment_id:         Optional[int] = None

    # Typed measurements
    electrical:           Optional["ElectricalMeasurements"] = None
    process_var:          Optional["ProcessVariables"] = None
    steam_fuel:           Optional["SteamFuelMeasurements"] = None
    water_agg:            Optional["WaterAggregate"] = None
    energy_agg:           Optional["EnergyAggregate"] = None

    # Original payload
    raw_payload:          str = ""


# ── Error record (goes to DLQ) ─────────────────────────────────────────────────
@dataclass
class ErrorRecord:
    kafka_topic:   str
    partition:     int
    offset:        int
    error_type:    str
    error_message: str
    raw_payload:   str
    processing_ts: datetime
