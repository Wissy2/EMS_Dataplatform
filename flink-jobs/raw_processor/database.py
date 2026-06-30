"""
database.py
TimescaleDB JDBC sink definitions targeting the normalized 'ems' schema.
"""

import logging
from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions

import config
from models import NormalisedRecord

log = logging.getLogger(__name__)


def _jdbc_opts() -> JdbcConnectionOptions:
    return (
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(config.TIMESCALE_JDBC_URL)
        .with_driver_name("org.postgresql.Driver")
        .with_user_name(config.TIMESCALE_USER)
        .with_password(config.TIMESCALE_PASSWORD)
        .build()
    )

def _exec_opts() -> JdbcExecutionOptions:
    return (
        JdbcExecutionOptions.builder()
        .with_batch_size(config.JDBC_BATCH_SIZE)
        .with_batch_interval_ms(config.JDBC_BATCH_INTERVAL_MS)
        .with_max_retries(config.JDBC_MAX_RETRIES)
        .build()
    )


# ── ems.raw_measurements ──────────────────────────────────────────────────────
RAW_MEASUREMENTS_SQL = """
INSERT INTO ems.raw_measurements
    (ingestion_timestamp, event_timestamp, kafka_topic, mqtt_topic, 
     kafka_partition, kafka_offset, message_key, plant, line, area, payload_type, payload)
VALUES (NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
"""

RAW_TYPES = Types.ROW([
    Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(),
    Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(),
    Types.STRING(), Types.STRING(), Types.STRING()
])

def raw_measurements_sink():
    return JdbcSink.sink(RAW_MEASUREMENTS_SQL, RAW_TYPES, _jdbc_opts(), _exec_opts())

def to_raw_row(record: NormalisedRecord) -> Row:
    return Row(
        record.event_time.isoformat(),
        record.kafka_topic,
        record.mqtt_topic or "",
        record.partition,
        record.offset,
        "",  # Message key
        record.plant,
        record.line,
        record.area,
        record.message_type.value,
        record.raw_payload
    )


# ── ems.electrical_measurements ───────────────────────────────────────────────
ELECTRICAL_SQL = """
INSERT INTO ems.electrical_measurements
    (timestamp, equipment_id, frequency, voltage_l1n, voltage_l2n, voltage_l3n,
     voltage_l1l2, voltage_l2l3, voltage_l3l1, current_l1, current_l2, current_l3,
     thd_voltage, thd_current, power_factor, active_power_kw, reactive_power_kvar,
     apparent_power_kva, energy_consumption_kwh, breaker_status, alarm_trip)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

ELECTRICAL_TYPES = Types.ROW([
    Types.STRING(), Types.INT(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
    Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
    Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
    Types.DOUBLE(), Types.BOOLEAN(), Types.BOOLEAN()
])

def electrical_measurements_sink():
    return JdbcSink.sink(ELECTRICAL_SQL, ELECTRICAL_TYPES, _jdbc_opts(), _exec_opts())

def to_electrical_row(record: NormalisedRecord) -> Row:
    e = record.electrical
    return Row(
        record.event_time.isoformat(), record.equipment_id, e.frequency,
        e.voltage_L1N, e.voltage_L2N, e.voltage_L3N, e.voltage_L1L2, e.voltage_L2L3, e.voltage_L3L1,
        e.current_L1, e.current_L2, e.current_L3, e.thd_voltage, e.thd_current, e.power_factor,
        e.active_power_kW, e.reactive_power_kVAR, e.apparent_power_kVA, e.energy_consumption_kWh,
        e.breaker_status, e.alarm_trip
    )


# ── ems.process_variables ─────────────────────────────────────────────────────
PROCESS_VAR_SQL = """
INSERT INTO ems.process_variables
    (timestamp, equipment_id, belt_speed, pump_speed, agitator_speed, flow, 
     instant_flow, temperature, volume_totalised_m3, air_pressure, air_flow, speed, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

PROCESS_VAR_TYPES = Types.ROW([
    Types.STRING(), Types.INT(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
    Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
    Types.BOOLEAN()
])

def process_variables_sink():
    return JdbcSink.sink(PROCESS_VAR_SQL, PROCESS_VAR_TYPES, _jdbc_opts(), _exec_opts())

def to_process_var_row(record: NormalisedRecord) -> Row:
    pv = record.process_var
    return Row(
        record.event_time.isoformat(), record.equipment_id, pv.belt_speed, pv.pump_speed, 
        pv.agitator_speed, pv.flow, pv.instant_flow, pv.temperature, pv.volume_totalised_m3, 
        pv.air_pressure, pv.air_flow, pv.speed, pv.status
    )


# ── ems.steam_fuel_measurements ───────────────────────────────────────────────
STEAM_FUEL_SQL = """
INSERT INTO ems.steam_fuel_measurements
    (timestamp, line_id, steam_flow_rate, steam_totalizer, steam_pressure, 
     steam_temperature, fuel_flow_rate, fuel_totalizer, fuel_pressure, fuel_temperature)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

STEAM_FUEL_TYPES = Types.ROW([
    Types.STRING(), Types.INT(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), 
    Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()
])

def steam_fuel_sink():
    return JdbcSink.sink(STEAM_FUEL_SQL, STEAM_FUEL_TYPES, _jdbc_opts(), _exec_opts())

def to_steam_fuel_row(record: NormalisedRecord) -> Row:
    s = record.steam_fuel
    return Row(
        record.event_time.isoformat(), record.line_id, s.steam_flow_rate, s.steam_totalizer, 
        s.steam_pressure, s.steam_temperature, s.fuel_flow_rate, s.fuel_totalizer, 
        s.fuel_pressure, s.fuel_temperature
    )


# ── ems.water_consumption ─────────────────────────────────────────────────────
WATER_SQL = """
INSERT INTO ems.water_consumption
    (timestamp, line_id, total_water_m3)
VALUES (?, ?, ?)
"""

WATER_TYPES = Types.ROW([Types.STRING(), Types.INT(), Types.DOUBLE()])

def water_consumption_sink():
    return JdbcSink.sink(WATER_SQL, WATER_TYPES, _jdbc_opts(), _exec_opts())

def to_water_row(record: NormalisedRecord) -> Row:
    w = record.water_agg
    return Row(
        record.event_time.isoformat(), record.line_id, 
        w.total_water_m3 if w else None
    )


# ── ems.energy_consumption ────────────────────────────────────────────────────
ENERGY_SQL = """
INSERT INTO ems.energy_consumption
    (timestamp, line_id, area_id, total_energy_kwh)
VALUES (?, ?, ?, ?)
"""

ENERGY_TYPES = Types.ROW([Types.STRING(), Types.INT(), Types.INT(), Types.DOUBLE()])

def energy_consumption_sink():
    return JdbcSink.sink(ENERGY_SQL, ENERGY_TYPES, _jdbc_opts(), _exec_opts())

def to_energy_row(record: NormalisedRecord) -> Row:
    e = record.energy_agg
    return Row(
        record.event_time.isoformat(), record.line_id, record.area_id, 
        e.total_energy_kWh if e else None
    )
