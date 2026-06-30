-- ============================================================
-- EMS DATABASE INITIALIZATION
-- PostgreSQL + TimescaleDB
-- ============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create schema
CREATE SCHEMA IF NOT EXISTS ems;
-- ============================================================
-- METADATA TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS ems.plants (
    plant_id SERIAL PRIMARY KEY,
    plant_name VARCHAR(100) NOT NULL UNIQUE,
    location VARCHAR(255),
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ems.production_lines (
    line_id SERIAL PRIMARY KEY,
    plant_id INTEGER NOT NULL REFERENCES ems.plants(plant_id) ON DELETE CASCADE,
    line_code VARCHAR(50) NOT NULL,
    description TEXT,

    UNIQUE (plant_id, line_code)
);

CREATE TABLE IF NOT EXISTS ems.areas (
    area_id SERIAL PRIMARY KEY,
    line_id INTEGER NOT NULL REFERENCES ems.production_lines(line_id) ON DELETE CASCADE,
    area_name VARCHAR(100) NOT NULL,

    UNIQUE (line_id, area_name)
);

CREATE TABLE IF NOT EXISTS ems.equipment (
    equipment_id SERIAL PRIMARY KEY,

    area_id INTEGER NOT NULL REFERENCES ems.areas(area_id) ON DELETE CASCADE,

    device_id VARCHAR(100) NOT NULL UNIQUE,

    equipment_name VARCHAR(150),

    equipment_type VARCHAR(100),

    mqtt_topic_template TEXT,

    manufacturer VARCHAR(100),

    rated_power_kw NUMERIC(10,2),

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- ELECTRICAL MEASUREMENTS
-- ============================================================

CREATE TABLE IF NOT EXISTS ems.electrical_measurements (

    timestamp TIMESTAMPTZ NOT NULL,

    equipment_id INTEGER NOT NULL REFERENCES ems.equipment(equipment_id),

    frequency DOUBLE PRECISION,

    voltage_l1n DOUBLE PRECISION,
    voltage_l2n DOUBLE PRECISION,
    voltage_l3n DOUBLE PRECISION,

    voltage_l1l2 DOUBLE PRECISION,
    voltage_l2l3 DOUBLE PRECISION,
    voltage_l3l1 DOUBLE PRECISION,

    current_l1 DOUBLE PRECISION,
    current_l2 DOUBLE PRECISION,
    current_l3 DOUBLE PRECISION,

    thd_voltage DOUBLE PRECISION,
    thd_current DOUBLE PRECISION,

    power_factor DOUBLE PRECISION,

    active_power_kw DOUBLE PRECISION,

    reactive_power_kvar DOUBLE PRECISION,

    apparent_power_kva DOUBLE PRECISION,

    energy_consumption_kwh DOUBLE PRECISION,

    breaker_status BOOLEAN,

    alarm_trip BOOLEAN
);

SELECT create_hypertable(
    'ems.electrical_measurements',
    'timestamp',
    if_not_exists => TRUE
);

-- ============================================================
-- PROCESS VARIABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS ems.process_variables (

    timestamp TIMESTAMPTZ NOT NULL,

    equipment_id INTEGER NOT NULL REFERENCES ems.equipment(equipment_id),

    belt_speed DOUBLE PRECISION,

    pump_speed DOUBLE PRECISION,

    agitator_speed DOUBLE PRECISION,

    flow DOUBLE PRECISION,

    instant_flow DOUBLE PRECISION,

    temperature DOUBLE PRECISION,

    volume_totalised_m3 DOUBLE PRECISION,

    air_pressure DOUBLE PRECISION,

    air_flow DOUBLE PRECISION,

    speed DOUBLE PRECISION,

    status BOOLEAN
);

SELECT create_hypertable(
    'ems.process_variables',
    'timestamp',
    if_not_exists => TRUE
);

-- ============================================================
-- STEAM / FUEL
-- ============================================================

CREATE TABLE IF NOT EXISTS ems.steam_fuel_measurements (

    timestamp TIMESTAMPTZ NOT NULL,

    line_id INTEGER REFERENCES ems.production_lines(line_id),

    steam_flow_rate DOUBLE PRECISION,

    steam_totalizer DOUBLE PRECISION,

    steam_pressure DOUBLE PRECISION,

    steam_temperature DOUBLE PRECISION,

    fuel_flow_rate DOUBLE PRECISION,

    fuel_totalizer DOUBLE PRECISION,

    fuel_pressure DOUBLE PRECISION,

    fuel_temperature DOUBLE PRECISION
);

SELECT create_hypertable(
    'ems.steam_fuel_measurements',
    'timestamp',
    if_not_exists => TRUE
);

-- ============================================================
-- TOTAL WATER CONSUMPTION
-- ============================================================

CREATE TABLE IF NOT EXISTS ems.water_consumption (

    timestamp TIMESTAMPTZ NOT NULL,

    line_id INTEGER REFERENCES ems.production_lines(line_id),

    total_water_m3 DOUBLE PRECISION
);

SELECT create_hypertable(
    'ems.water_consumption',
    'timestamp',
    if_not_exists => TRUE
);

-- ============================================================
-- TOTAL ENERGY CONSUMPTION
-- ============================================================

CREATE TABLE IF NOT EXISTS ems.energy_consumption (

    timestamp TIMESTAMPTZ NOT NULL,

    line_id INTEGER REFERENCES ems.production_lines(line_id),

    area_id INTEGER REFERENCES ems.areas(area_id),

    total_energy_kwh DOUBLE PRECISION
);

SELECT create_hypertable(
    'ems.energy_consumption',
    'timestamp',
    if_not_exists => TRUE
);

-- ============================================================
-- EQUIPMENT KPI
-- ============================================================

CREATE TABLE IF NOT EXISTS ems.equipment_kpis (

    timestamp TIMESTAMPTZ NOT NULL,

    equipment_id INTEGER REFERENCES ems.equipment(equipment_id),

    energy_used_kwh DOUBLE PRECISION,

    runtime_minutes DOUBLE PRECISION,

    availability DOUBLE PRECISION,

    downtime_minutes DOUBLE PRECISION,

    average_power_kw DOUBLE PRECISION,

    alarm_count INTEGER
);

SELECT create_hypertable(
    'ems.equipment_kpis',
    'timestamp',
    if_not_exists => TRUE
);

-- ============================================================
-- AREA KPI
-- ============================================================

CREATE TABLE IF NOT EXISTS ems.area_kpis (

    timestamp TIMESTAMPTZ NOT NULL,

    area_id INTEGER REFERENCES ems.areas(area_id),

    energy_kwh DOUBLE PRECISION,

    water_m3 DOUBLE PRECISION,

    average_power_kw DOUBLE PRECISION,

    steam_used DOUBLE PRECISION,

    fuel_used DOUBLE PRECISION,

    co2_emission_kg DOUBLE PRECISION,

    specific_energy_consumption DOUBLE PRECISION
);

SELECT create_hypertable(
    'ems.area_kpis',
    'timestamp',
    if_not_exists => TRUE
);

-- ============================================================
-- LINE KPI
-- ============================================================

CREATE TABLE IF NOT EXISTS ems.line_kpis (

    timestamp TIMESTAMPTZ NOT NULL,

    line_id INTEGER REFERENCES ems.production_lines(line_id),

    energy_kwh DOUBLE PRECISION,

    water_m3 DOUBLE PRECISION,

    steam_used DOUBLE PRECISION,

    fuel_used DOUBLE PRECISION,

    co2_emission_kg DOUBLE PRECISION,

    specific_energy_consumption DOUBLE PRECISION
);

SELECT create_hypertable(
    'ems.line_kpis',
    'timestamp',
    if_not_exists => TRUE
);

-- ============================================================
-- PLANT KPI
-- ============================================================

CREATE TABLE IF NOT EXISTS ems.plant_kpis (

    timestamp TIMESTAMPTZ NOT NULL,

    plant_id INTEGER REFERENCES ems.plants(plant_id),

    energy_kwh DOUBLE PRECISION,

    water_m3 DOUBLE PRECISION,

    steam_used DOUBLE PRECISION,

    fuel_used DOUBLE PRECISION,

    co2_emission_kg DOUBLE PRECISION,

    specific_energy_consumption DOUBLE PRECISION
);

SELECT create_hypertable(
    'ems.plant_kpis',
    'timestamp',
    if_not_exists => TRUE
);

-- ============================================================
-- ALARM HISTORY
-- ============================================================

CREATE TABLE IF NOT EXISTS ems.alarm_history (

    timestamp TIMESTAMPTZ NOT NULL,

    equipment_id INTEGER REFERENCES ems.equipment(equipment_id),

    alarm_code VARCHAR(50),

    severity VARCHAR(20),

    state BOOLEAN,

    description TEXT
);

SELECT create_hypertable(
    'ems.alarm_history',
    'timestamp',
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS ems.raw_measurements (

    ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),

    event_timestamp TIMESTAMPTZ,

    kafka_topic TEXT NOT NULL,

    mqtt_topic TEXT,

    kafka_partition INTEGER,

    kafka_offset BIGINT,

    message_key TEXT,

    plant TEXT,

    line TEXT,

    area TEXT,

    payload_type TEXT,

    payload JSONB NOT NULL
);

SELECT create_hypertable(
    'ems.raw_measurements',
    'ingestion_timestamp',
    if_not_exists => TRUE
);

-- Grant privileges to ems_user
GRANT ALL PRIVILEGES ON SCHEMA ems TO ems_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ems TO ems_user;

-- ============================================================
-- INSERT INITIAL METADATA
-- ============================================================

INSERT INTO ems.plants (plant_name, location)
VALUES ('Al_Youssoufia_Plant', 'Youssoufia')
ON CONFLICT DO NOTHING;

INSERT INTO ems.production_lines (plant_id, line_code)
VALUES (1, 'Line-1')
ON CONFLICT DO NOTHING;
