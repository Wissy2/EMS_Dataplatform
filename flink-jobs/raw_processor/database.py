"""
database.py
JDBC sink builders for all ems.* TimescaleDB tables.
line_id / area_id are INTEGER FKs; equipment is now referenced via
tag_id (VARCHAR, e.g. '310L2-AG-01'), matching the updated schema.
"""

import logging
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.typeinfo import Types

import config
from models import NormalisedRecord

log = logging.getLogger(__name__)

# ── TypeInformation Schemas (Global Constants) ──────────────────────────────
# Used in main.py via .returns(TYPE_CONSTANT)
# Define these at the module level so main.py can see them
RAW_TYPE = Types.ROW([
    Types.STRING(),  # event_timestamp
    Types.STRING(),  # kafka_topic
    Types.STRING(),  # mqtt_topic
    Types.INT(),     # kafka_partition
    Types.LONG(),    # kafka_offset
    Types.STRING(),  # message_key (device_id)
    Types.STRING(),  # plant
    Types.STRING(),  # line
    Types.STRING(),  # area
    Types.STRING(),  # payload_type
    Types.STRING(),  # payload
])

ELECTRICAL_TYPE = Types.ROW([
    Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), 
    Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), 
    Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), 
    Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.BOOLEAN(), Types.BOOLEAN()
])

PROCESS_VAR_TYPE = Types.ROW([
    Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), 
    Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), 
    Types.DOUBLE(), Types.DOUBLE(), Types.BOOLEAN()
])

STEAM_FUEL_TYPE = Types.ROW([
    Types.STRING(), Types.INT(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), 
    Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()
])

WATER_TYPE = Types.ROW([Types.STRING(), Types.INT(), Types.DOUBLE()])

ENERGY_TYPE = Types.ROW([Types.STRING(), Types.INT(), Types.INT(), Types.DOUBLE()])

# ── Connection Helpers ──────────────────────────────────────────────────────

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

# ── Sink builders: correct arg order, no setter callback ────────────────────
RAW_SQL = """
INSERT INTO ems.raw_measurements
    (ingestion_timestamp, event_timestamp,
     kafka_topic, mqtt_topic, kafka_partition, kafka_offset,
     message_key, plant, line, area, payload_type, payload)
VALUES
    (NOW(), ?::timestamptz,
     ?, ?, ?, ?,
     ?, ?, ?, ?, ?, ?::jsonb)
ON CONFLICT DO NOTHING
"""
def raw_measurements_sink():
    return JdbcSink.sink(RAW_SQL, RAW_TYPE, _jdbc_opts(), _exec_opts())
    
ELECTRICAL_SQL = """
INSERT INTO ems.electrical_measurements
    (timestamp, tag_id,
     frequency,
     voltage_l1n, voltage_l2n, voltage_l3n,
     voltage_l1l2, voltage_l2l3, voltage_l3l1,
     current_l1, current_l2, current_l3,
     thd_voltage, thd_current, power_factor,
     active_power_kw, reactive_power_kvar, apparent_power_kva,
     energy_consumption_kwh, breaker_status, alarm_trip)
VALUES
    (?::timestamptz, ?,
     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
"""
def electrical_measurements_sink():
    return JdbcSink.sink(ELECTRICAL_SQL, ELECTRICAL_TYPE, _jdbc_opts(), _exec_opts())
    
# ── ems.process_variables ─────────────────────────────────────────────────────
PROCESS_VAR_SQL = """
INSERT INTO ems.process_variables
    (timestamp, tag_id,
     belt_speed, pump_speed, agitator_speed,
     flow, instant_flow, temperature,
     volume_totalised_m3, air_pressure, air_flow,
     speed, status)
VALUES
    (?::timestamptz, ?,
     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
"""
def process_variables_sink():
    return JdbcSink.sink(PROCESS_VAR_SQL, PROCESS_VAR_TYPE, _jdbc_opts(), _exec_opts())

# ── ems.steam_fuel_measurements ───────────────────────────────────────────────
STEAM_FUEL_SQL = """
INSERT INTO ems.steam_fuel_measurements
    (timestamp, line_id,
     steam_flow_rate, steam_totalizer, steam_pressure, steam_temperature,
     fuel_flow_rate, fuel_totalizer, fuel_pressure, fuel_temperature)
VALUES
    (?::timestamptz, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
"""
def steam_fuel_sink():
    return JdbcSink.sink(STEAM_FUEL_SQL, STEAM_FUEL_TYPE, _jdbc_opts(), _exec_opts())

# ── ems.water_consumption ─────────────────────────────────────────────────────
WATER_SQL = """
INSERT INTO ems.water_consumption
    (timestamp, line_id, total_water_m3)
VALUES (?::timestamptz, ?, ?)
ON CONFLICT DO NOTHING
"""
def water_consumption_sink():
    return JdbcSink.sink(WATER_SQL, WATER_TYPE, _jdbc_opts(), _exec_opts())

# ── ems.energy_consumption ────────────────────────────────────────────────────
ENERGY_SQL = """
INSERT INTO ems.energy_consumption
    (timestamp, line_id, area_id, total_energy_kwh)
VALUES (?::timestamptz, ?, ?, ?)
ON CONFLICT DO NOTHING
"""
def energy_consumption_sink():
    return JdbcSink.sink(ENERGY_SQL, ENERGY_TYPE, _jdbc_opts(), _exec_opts())
    
    
    
from pyflink.common import Row

#Row_mappers
def to_raw_row(r: NormalisedRecord) -> Row:
    return Row(
        r.event_time.isoformat(),
        r.kafka_topic,
        r.mqtt_topic or "",
        r.partition,
        r.offset,
        r.device_id or "",
        r.plant,
        r.line,
        r.area,
        r.message_type.value,
        r.raw_payload,
    )

def to_electrical_row(r: NormalisedRecord) -> Row:
    e = r.electrical
    return Row(
        r.event_time.isoformat(), r.ids.tag_id,
        e.frequency, e.voltage_l1n, e.voltage_l2n, e.voltage_l3n,
        e.voltage_l1l2, e.voltage_l2l3, e.voltage_l3l1,
        e.current_l1, e.current_l2, e.current_l3,
        e.thd_voltage, e.thd_current, e.power_factor,
        e.active_power_kw, e.reactive_power_kvar, e.apparent_power_kva,
        e.energy_consumption_kwh, e.breaker_status, e.alarm_trip,
    )

def to_process_var_row(r: NormalisedRecord) -> Row:
    pv = r.process_var
    return Row(
        r.event_time.isoformat(), r.ids.tag_id,
        pv.belt_speed, pv.pump_speed, pv.agitator_speed, pv.flow,
        pv.instant_flow, pv.temperature, pv.volume_totalised_m3,
        pv.air_pressure, pv.air_flow, pv.speed, pv.status,
    )

def to_steam_fuel_row(r: NormalisedRecord) -> Row:
    s = r.steam_fuel
    return Row(
        r.event_time.isoformat(), r.ids.line_id,
        s.steam_flow_rate, s.steam_totalizer, s.steam_pressure, s.steam_temperature,
        s.fuel_flow_rate, s.fuel_totalizer, s.fuel_pressure, s.fuel_temperature,
    )

def to_water_row(r: NormalisedRecord) -> Row:
    return Row(
        r.event_time.isoformat(), r.ids.line_id,
        r.water_agg.total_water_m3 if r.water_agg else None,
    )

def to_energy_row(r: NormalisedRecord) -> Row:
    return Row(
        r.event_time.isoformat(), r.ids.line_id, r.ids.area_id,
        r.energy_agg.total_energy_kwh if r.energy_agg else None,
    )
