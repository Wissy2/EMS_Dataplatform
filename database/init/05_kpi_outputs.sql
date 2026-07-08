-- ============================================================
-- EMS KPI OUTPUT TABLES
-- Wide tables consumed by the Analytics/KPI Flink job.
-- ============================================================

BEGIN;

DROP TABLE IF EXISTS ems.equipment_kpis CASCADE;
DROP TABLE IF EXISTS ems.area_kpis CASCADE;
DROP TABLE IF EXISTS ems.line_kpis CASCADE;
DROP TABLE IF EXISTS ems.plant_kpis CASCADE;

CREATE TABLE ems.equipment_kpis (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    tag_id VARCHAR(50) NOT NULL REFERENCES ems.equipment(tag_id),
    plant_id INTEGER REFERENCES ems.plants(plant_id),
    line_id INTEGER REFERENCES ems.production_lines(line_id),
    area_id INTEGER REFERENCES ems.areas(area_id),
    energy_delta_kwh DOUBLE PRECISION,
    total_energy_kwh DOUBLE PRECISION,
    energy_cost DOUBLE PRECISION,
    co2_kg DOUBLE PRECISION,
    active_power_avg_kw DOUBLE PRECISION,
    active_power_min_kw DOUBLE PRECISION,
    active_power_max_kw DOUBLE PRECISION,
    apparent_power_avg_kva DOUBLE PRECISION,
    reactive_power_avg_kvar DOUBLE PRECISION,
    peak_demand_kw DOUBLE PRECISION,
    load_factor DOUBLE PRECISION,
    avg_voltage_ln DOUBLE PRECISION,
    voltage_stability_cv DOUBLE PRECISION,
    voltage_unbalance_percent DOUBLE PRECISION,
    avg_current_a DOUBLE PRECISION,
    min_current_a DOUBLE PRECISION,
    max_current_a DOUBLE PRECISION,
    current_unbalance_percent DOUBLE PRECISION,
    avg_frequency_hz DOUBLE PRECISION,
    min_frequency_hz DOUBLE PRECISION,
    max_frequency_hz DOUBLE PRECISION,
    avg_power_factor DOUBLE PRECISION,
    min_power_factor DOUBLE PRECISION,
    power_factor_trend DOUBLE PRECISION,
    power_factor_quality_flag TEXT,
    avg_thd_voltage DOUBLE PRECISION,
    max_thd_voltage DOUBLE PRECISION,
    thd_voltage_trend DOUBLE PRECISION,
    avg_thd_current DOUBLE PRECISION,
    max_thd_current DOUBLE PRECISION,
    thd_current_trend DOUBLE PRECISION,
    runtime_seconds DOUBLE PRECISION,
    stopped_seconds DOUBLE PRECISION,
    availability_percent DOUBLE PRECISION,
    utilization_percent DOUBLE PRECISION,
    demand_factor DOUBLE PRECISION,
    reactive_power_ratio DOUBLE PRECISION,
    apparent_power_utilization DOUBLE PRECISION,
    rolling_15m_peak_kw DOUBLE PRECISION,
    rolling_60m_energy_kwh DOUBLE PRECISION,
    voltage_quality_index DOUBLE PRECISION,
    power_quality_index DOUBLE PRECISION,
    energy_trend DOUBLE PRECISION,
    co2_trend DOUBLE PRECISION,
    energy_cost_trend DOUBLE PRECISION,
    avg_belt_speed DOUBLE PRECISION,
    max_belt_speed DOUBLE PRECISION,
    avg_pump_speed DOUBLE PRECISION,
    avg_air_flow DOUBLE PRECISION,
    avg_air_pressure DOUBLE PRECISION,
    avg_flow DOUBLE PRECISION,
    avg_temperature DOUBLE PRECISION,
    avg_agitator_speed DOUBLE PRECISION,
    process_running_percent DOUBLE PRECISION,
    equipment_health_score DOUBLE PRECISION
);

SELECT create_hypertable('ems.equipment_kpis', 'window_end', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS equipment_kpis_window_tag_uidx
    ON ems.equipment_kpis (window_end, tag_id);

CREATE TABLE ems.area_kpis (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    area_id INTEGER NOT NULL REFERENCES ems.areas(area_id),
    plant_id INTEGER REFERENCES ems.plants(plant_id),
    line_id INTEGER REFERENCES ems.production_lines(line_id),
    energy_kwh DOUBLE PRECISION,
    energy_cost DOUBLE PRECISION,
    co2_kg DOUBLE PRECISION,
    average_power_kw DOUBLE PRECISION,
    peak_demand_kw DOUBLE PRECISION,
    load_factor DOUBLE PRECISION,
    running_equipment_count BIGINT,
    equipment_count BIGINT,
    availability_percent DOUBLE PRECISION,
    water_m3 DOUBLE PRECISION,
    water_cost DOUBLE PRECISION,
    steam_consumption DOUBLE PRECISION,
    fuel_consumption DOUBLE PRECISION,
    fuel_cost DOUBLE PRECISION
);

SELECT create_hypertable('ems.area_kpis', 'window_end', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS area_kpis_window_area_uidx
    ON ems.area_kpis (window_end, area_id);

CREATE TABLE ems.line_kpis (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    line_id INTEGER NOT NULL REFERENCES ems.production_lines(line_id),
    plant_id INTEGER REFERENCES ems.plants(plant_id),
    energy_kwh DOUBLE PRECISION,
    energy_cost DOUBLE PRECISION,
    co2_kg DOUBLE PRECISION,
    average_power_kw DOUBLE PRECISION,
    peak_demand_kw DOUBLE PRECISION,
    load_factor DOUBLE PRECISION,
    water_m3 DOUBLE PRECISION,
    water_cost DOUBLE PRECISION,
    steam_consumption DOUBLE PRECISION,
    avg_steam_pressure DOUBLE PRECISION,
    avg_steam_temperature DOUBLE PRECISION,
    max_steam_flow DOUBLE PRECISION,
    fuel_consumption DOUBLE PRECISION,
    avg_fuel_pressure DOUBLE PRECISION,
    avg_fuel_temperature DOUBLE PRECISION,
    max_fuel_flow DOUBLE PRECISION,
    fuel_cost DOUBLE PRECISION
);

SELECT create_hypertable('ems.line_kpis', 'window_end', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS line_kpis_window_line_uidx
    ON ems.line_kpis (window_end, line_id);

CREATE TABLE ems.plant_kpis (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    plant_id INTEGER NOT NULL REFERENCES ems.plants(plant_id),
    energy_kwh DOUBLE PRECISION,
    energy_cost DOUBLE PRECISION,
    co2_kg DOUBLE PRECISION,
    daily_co2_kg DOUBLE PRECISION,
    monthly_co2_kg DOUBLE PRECISION,
    average_power_kw DOUBLE PRECISION,
    peak_demand_kw DOUBLE PRECISION,
    daily_peak_kw DOUBLE PRECISION,
    monthly_peak_kw DOUBLE PRECISION,
    load_factor DOUBLE PRECISION,
    water_m3 DOUBLE PRECISION,
    water_cost DOUBLE PRECISION,
    daily_water_m3 DOUBLE PRECISION,
    monthly_water_m3 DOUBLE PRECISION,
    steam_consumption DOUBLE PRECISION,
    fuel_consumption DOUBLE PRECISION,
    fuel_cost DOUBLE PRECISION
);

SELECT create_hypertable('ems.plant_kpis', 'window_end', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS plant_kpis_window_plant_uidx
    ON ems.plant_kpis (window_end, plant_id);

ALTER TABLE ems.energy_consumption
    ADD COLUMN IF NOT EXISTS window_start TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS window_end TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS aggregation_level TEXT,
    ADD COLUMN IF NOT EXISTS aggregation_id TEXT,
    ADD COLUMN IF NOT EXISTS plant_id INTEGER REFERENCES ems.plants(plant_id),
    ADD COLUMN IF NOT EXISTS tag_id VARCHAR(50) REFERENCES ems.equipment(tag_id),
    ADD COLUMN IF NOT EXISTS energy_delta_kwh DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS energy_cost DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS co2_kg DOUBLE PRECISION;

CREATE UNIQUE INDEX IF NOT EXISTS energy_consumption_kpi_uidx
    ON ems.energy_consumption (timestamp, aggregation_level, aggregation_id)
    WHERE aggregation_level IS NOT NULL AND aggregation_id IS NOT NULL;

GRANT ALL PRIVILEGES ON ems.equipment_kpis TO ems_user;
GRANT ALL PRIVILEGES ON ems.area_kpis TO ems_user;
GRANT ALL PRIVILEGES ON ems.line_kpis TO ems_user;
GRANT ALL PRIVILEGES ON ems.plant_kpis TO ems_user;
GRANT ALL PRIVILEGES ON ems.energy_consumption TO ems_user;

COMMIT;
