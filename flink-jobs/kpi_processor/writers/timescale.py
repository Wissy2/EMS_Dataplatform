"""TimescaleDB JDBC sink DDL for KPI outputs."""

from __future__ import annotations

from config.settings import KpiSettings


def _jdbc_options(settings: KpiSettings) -> str:
    return f"""
        'connector' = 'jdbc',
        'url' = '{settings.timescale_jdbc_url}',
        'driver' = 'org.postgresql.Driver',
        'username' = '{settings.timescale_user}',
        'password' = '{settings.timescale_password}',
        'sink.buffer-flush.max-rows' = '{settings.jdbc_batch_size}',
        'sink.buffer-flush.interval' = '{settings.jdbc_batch_interval_ms}ms',
        'sink.max-retries' = '{settings.jdbc_max_retries}'
    """


def _jdbc_options_for(settings: KpiSettings, table_name: str) -> str:
    base_options = _jdbc_options(settings).strip()
    return f"""
        'table-name' = '{table_name}',
        {base_options}
    """


def register_kpi_sinks(t_env, settings: KpiSettings) -> None:
    sink_ddls = [
        f"""
        CREATE TABLE equipment_kpis_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            tag_id STRING,
            plant_id INT,
            line_id INT,
            area_id INT,
            energy_delta_kwh DOUBLE,
            total_energy_kwh DOUBLE,
            energy_cost DOUBLE,
            co2_kg DOUBLE,
            active_power_avg_kw DOUBLE,
            active_power_min_kw DOUBLE,
            active_power_max_kw DOUBLE,
            apparent_power_avg_kva DOUBLE,
            reactive_power_avg_kvar DOUBLE,
            peak_demand_kw DOUBLE,
            load_factor DOUBLE,
            avg_voltage_ln DOUBLE,
            voltage_stability_cv DOUBLE,
            voltage_unbalance_percent DOUBLE,
            avg_current_a DOUBLE,
            min_current_a DOUBLE,
            max_current_a DOUBLE,
            current_unbalance_percent DOUBLE,
            avg_frequency_hz DOUBLE,
            min_frequency_hz DOUBLE,
            max_frequency_hz DOUBLE,
            avg_power_factor DOUBLE,
            min_power_factor DOUBLE,
            power_factor_trend DOUBLE,
            power_factor_quality_flag STRING,
            avg_thd_voltage DOUBLE,
            max_thd_voltage DOUBLE,
            thd_voltage_trend DOUBLE,
            avg_thd_current DOUBLE,
            max_thd_current DOUBLE,
            thd_current_trend DOUBLE,
            runtime_seconds DOUBLE,
            stopped_seconds DOUBLE,
            availability_percent DOUBLE,
            utilization_percent DOUBLE,
            demand_factor DOUBLE,
            reactive_power_ratio DOUBLE,
            apparent_power_utilization DOUBLE,
            rolling_15m_peak_kw DOUBLE,
            rolling_60m_energy_kwh DOUBLE,
            voltage_quality_index DOUBLE,
            power_quality_index DOUBLE,
            energy_trend DOUBLE,
            co2_trend DOUBLE,
            energy_cost_trend DOUBLE,
            avg_belt_speed DOUBLE,
            max_belt_speed DOUBLE,
            avg_pump_speed DOUBLE,
            avg_air_flow DOUBLE,
            avg_air_pressure DOUBLE,
            avg_flow DOUBLE,
            avg_temperature DOUBLE,
            avg_agitator_speed DOUBLE,
            process_running_percent DOUBLE,
            equipment_health_score DOUBLE,
            PRIMARY KEY (window_end, tag_id) NOT ENFORCED
        ) WITH ({_jdbc_options_for(settings, "ems.equipment_kpis")})
        """,
        f"""
        CREATE TABLE area_kpis_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            area_id INT,
            plant_id INT,
            line_id INT,
            energy_kwh DOUBLE,
            energy_cost DOUBLE,
            co2_kg DOUBLE,
            average_power_kw DOUBLE,
            peak_demand_kw DOUBLE,
            load_factor DOUBLE,
            running_equipment_count BIGINT,
            equipment_count BIGINT,
            availability_percent DOUBLE,
            water_m3 DOUBLE,
            water_cost DOUBLE,
            steam_consumption DOUBLE,
            fuel_consumption DOUBLE,
            fuel_cost DOUBLE,
            PRIMARY KEY (window_end, area_id) NOT ENFORCED
        ) WITH ({_jdbc_options_for(settings, "ems.area_kpis")})
        """,
        f"""
        CREATE TABLE line_kpis_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            line_id INT,
            plant_id INT,
            energy_kwh DOUBLE,
            energy_cost DOUBLE,
            co2_kg DOUBLE,
            average_power_kw DOUBLE,
            peak_demand_kw DOUBLE,
            load_factor DOUBLE,
            water_m3 DOUBLE,
            water_cost DOUBLE,
            steam_consumption DOUBLE,
            avg_steam_pressure DOUBLE,
            avg_steam_temperature DOUBLE,
            max_steam_flow DOUBLE,
            fuel_consumption DOUBLE,
            avg_fuel_pressure DOUBLE,
            avg_fuel_temperature DOUBLE,
            max_fuel_flow DOUBLE,
            fuel_cost DOUBLE,
            PRIMARY KEY (window_end, line_id) NOT ENFORCED
        ) WITH ({_jdbc_options_for(settings, "ems.line_kpis")})
        """,
        f"""
        CREATE TABLE plant_kpis_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            plant_id INT,
            energy_kwh DOUBLE,
            energy_cost DOUBLE,
            co2_kg DOUBLE,
            daily_co2_kg DOUBLE,
            monthly_co2_kg DOUBLE,
            average_power_kw DOUBLE,
            peak_demand_kw DOUBLE,
            daily_peak_kw DOUBLE,
            monthly_peak_kw DOUBLE,
            load_factor DOUBLE,
            water_m3 DOUBLE,
            water_cost DOUBLE,
            daily_water_m3 DOUBLE,
            monthly_water_m3 DOUBLE,
            steam_consumption DOUBLE,
            fuel_consumption DOUBLE,
            fuel_cost DOUBLE,
            PRIMARY KEY (window_end, plant_id) NOT ENFORCED
        ) WITH ({_jdbc_options_for(settings, "ems.plant_kpis")})
        """,
        f"""
        CREATE TABLE energy_consumption_kpi_sink (
            `timestamp` TIMESTAMP(3),
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            aggregation_level STRING,
            aggregation_id STRING,
            plant_id INT,
            line_id INT,
            area_id INT,
            tag_id STRING,
            energy_delta_kwh DOUBLE,
            total_energy_kwh DOUBLE,
            energy_cost DOUBLE,
            co2_kg DOUBLE,
            PRIMARY KEY (`timestamp`, aggregation_level, aggregation_id) NOT ENFORCED
        ) WITH ({_jdbc_options_for(settings, "ems.energy_consumption")})
        """,
    ]
    for ddl in sink_ddls:
        t_env.execute_sql(ddl)
