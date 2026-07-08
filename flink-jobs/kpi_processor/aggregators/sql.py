"""Flink SQL source and aggregation definitions for EMS KPIs."""

from __future__ import annotations

from config.settings import KpiSettings


def _startup_mode(settings: KpiSettings) -> str:
    return "earliest-offset" if settings.kafka_start_offset == "earliest" else "latest-offset"


def register_sources(t_env, settings: KpiSettings) -> None:
    startup = _startup_mode(settings)
    broker = settings.kafka_broker
    group = settings.kafka_group_id
    topics = settings.topics
    ddls = [
        f"""
        CREATE TABLE electrical_src (
            event_time TIMESTAMP(3),
            tag_id STRING,
            plant_id INT,
            line_id INT,
            area_id INT,
            frequency DOUBLE,
            voltage_l1n DOUBLE,
            voltage_l2n DOUBLE,
            voltage_l3n DOUBLE,
            current_l1 DOUBLE,
            current_l2 DOUBLE,
            current_l3 DOUBLE,
            thd_voltage DOUBLE,
            thd_current DOUBLE,
            power_factor DOUBLE,
            active_power_kw DOUBLE,
            reactive_power_kvar DOUBLE,
            apparent_power_kva DOUBLE,
            energy_consumption_kwh DOUBLE,
            breaker_status BOOLEAN,
            alarm_trip BOOLEAN,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topics["electrical"]}',
            'properties.bootstrap.servers' = '{broker}',
            'properties.group.id' = '{group}',
            'scan.startup.mode' = '{startup}',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
        """,
        f"""
        CREATE TABLE process_var_src (
            event_time TIMESTAMP(3),
            tag_id STRING,
            plant_id INT,
            line_id INT,
            area_id INT,
            belt_speed DOUBLE,
            pump_speed DOUBLE,
            agitator_speed DOUBLE,
            flow DOUBLE,
            instant_flow DOUBLE,
            temperature DOUBLE,
            air_pressure DOUBLE,
            air_flow DOUBLE,
            speed DOUBLE,
            status BOOLEAN,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topics["process_variables"]}',
            'properties.bootstrap.servers' = '{broker}',
            'properties.group.id' = '{group}-pv',
            'scan.startup.mode' = '{startup}',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
        """,
        f"""
        CREATE TABLE steam_fuel_src (
            event_time TIMESTAMP(3),
            plant_id INT,
            line_id INT,
            steam_flow_rate DOUBLE,
            steam_totalizer DOUBLE,
            steam_pressure DOUBLE,
            steam_temperature DOUBLE,
            fuel_flow_rate DOUBLE,
            fuel_totalizer DOUBLE,
            fuel_pressure DOUBLE,
            fuel_temperature DOUBLE,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topics["steam_fuel"]}',
            'properties.bootstrap.servers' = '{broker}',
            'properties.group.id' = '{group}-steam-fuel',
            'scan.startup.mode' = '{startup}',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
        """,
        f"""
        CREATE TABLE water_src (
            event_time TIMESTAMP(3),
            plant_id INT,
            line_id INT,
            total_water_m3 DOUBLE,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topics["water"]}',
            'properties.bootstrap.servers' = '{broker}',
            'properties.group.id' = '{group}-water',
            'scan.startup.mode' = '{startup}',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
        """,
    ]
    for ddl in ddls:
        t_env.execute_sql(ddl)


def register_views(t_env, settings: KpiSettings) -> None:
    thresholds = settings.alarm_thresholds
    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW electrical_delta AS
        SELECT *,
            CASE
                WHEN prev_energy IS NULL THEN 0.0
                WHEN energy_consumption_kwh IS NULL THEN 0.0
                WHEN energy_consumption_kwh >= prev_energy THEN energy_consumption_kwh - prev_energy
                ELSE energy_consumption_kwh
            END AS energy_delta_kwh,
            COALESCE(apparent_power_kva, ABS(active_power_kw / NULLIF(power_factor, 0))) AS calc_apparent_power_kva,
            COALESCE(reactive_power_kvar, SQRT(GREATEST(POWER(COALESCE(apparent_power_kva, ABS(active_power_kw / NULLIF(power_factor, 0))), 2) - POWER(active_power_kw, 2), 0))) AS calc_reactive_power_kvar,
            (voltage_l1n + voltage_l2n + voltage_l3n) / 3.0 AS avg_phase_voltage,
            (current_l1 + current_l2 + current_l3) / 3.0 AS avg_phase_current,
            CASE WHEN ((voltage_l1n + voltage_l2n + voltage_l3n) / 3.0) = 0 THEN NULL ELSE
                GREATEST(ABS(voltage_l1n - ((voltage_l1n + voltage_l2n + voltage_l3n) / 3.0)),
                         ABS(voltage_l2n - ((voltage_l1n + voltage_l2n + voltage_l3n) / 3.0)),
                         ABS(voltage_l3n - ((voltage_l1n + voltage_l2n + voltage_l3n) / 3.0)))
                / ((voltage_l1n + voltage_l2n + voltage_l3n) / 3.0) * 100.0 END AS voltage_unbalance_percent,
            CASE WHEN ((current_l1 + current_l2 + current_l3) / 3.0) = 0 THEN NULL ELSE
                GREATEST(ABS(current_l1 - ((current_l1 + current_l2 + current_l3) / 3.0)),
                         ABS(current_l2 - ((current_l1 + current_l2 + current_l3) / 3.0)),
                         ABS(current_l3 - ((current_l1 + current_l2 + current_l3) / 3.0)))
                / ((current_l1 + current_l2 + current_l3) / 3.0) * 100.0 END AS current_unbalance_percent
        FROM (
            SELECT e.*,
                LAG(energy_consumption_kwh) OVER (PARTITION BY tag_id ORDER BY event_time) AS prev_energy
            FROM electrical_src e
        )
        """
    )

    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW water_delta AS
        SELECT *,
            CASE
                WHEN prev_water IS NULL THEN 0.0
                WHEN total_water_m3 IS NULL THEN 0.0
                WHEN total_water_m3 >= prev_water THEN total_water_m3 - prev_water
                ELSE total_water_m3
            END AS water_delta_m3
        FROM (
            SELECT w.*,
                LAG(total_water_m3) OVER (PARTITION BY line_id ORDER BY event_time) AS prev_water
            FROM water_src w
        )
        """
    )

    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW steam_fuel_delta AS
        SELECT *,
            CASE
                WHEN prev_steam IS NULL THEN 0.0
                WHEN steam_totalizer IS NULL THEN 0.0
                WHEN steam_totalizer >= prev_steam THEN steam_totalizer - prev_steam
                ELSE steam_totalizer
            END AS steam_delta,
            CASE
                WHEN prev_fuel IS NULL THEN 0.0
                WHEN fuel_totalizer IS NULL THEN 0.0
                WHEN fuel_totalizer >= prev_fuel THEN fuel_totalizer - prev_fuel
                ELSE fuel_totalizer
            END AS fuel_delta
        FROM (
            SELECT s.*,
                LAG(steam_totalizer) OVER (PARTITION BY line_id ORDER BY event_time) AS prev_steam,
                LAG(fuel_totalizer) OVER (PARTITION BY line_id ORDER BY event_time) AS prev_fuel
            FROM steam_fuel_src s
        )
        """
    )

    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW equipment_window AS
        SELECT
            window_start,
            window_end,
            tag_id,
            MAX(plant_id) AS plant_id,
            MAX(line_id) AS line_id,
            MAX(area_id) AS area_id,
            SUM(energy_delta_kwh) AS energy_delta_kwh,
            MAX(energy_consumption_kwh) AS total_energy_kwh,
            SUM(energy_delta_kwh) * {settings.electricity_tariff_per_kwh} AS energy_cost,
            SUM(energy_delta_kwh) * {settings.co2_emission_factor_kg_per_kwh} AS co2_kg,
            AVG(active_power_kw) AS active_power_avg_kw,
            MIN(active_power_kw) AS active_power_min_kw,
            MAX(active_power_kw) AS active_power_max_kw,
            AVG(calc_apparent_power_kva) AS apparent_power_avg_kva,
            AVG(calc_reactive_power_kvar) AS reactive_power_avg_kvar,
            MAX(active_power_kw) AS peak_demand_kw,
            AVG(active_power_kw) / NULLIF(MAX(active_power_kw), 0) AS load_factor,
            AVG(avg_phase_voltage) AS avg_voltage_ln,
            STDDEV_POP(avg_phase_voltage) / NULLIF(AVG(avg_phase_voltage), 0) AS voltage_stability_cv,
            AVG(voltage_unbalance_percent) AS voltage_unbalance_percent,
            AVG(avg_phase_current) AS avg_current_a,
            LEAST(MIN(current_l1), MIN(current_l2), MIN(current_l3)) AS min_current_a,
            GREATEST(MAX(current_l1), MAX(current_l2), MAX(current_l3)) AS max_current_a,
            AVG(current_unbalance_percent) AS current_unbalance_percent,
            AVG(frequency) AS avg_frequency_hz,
            MIN(frequency) AS min_frequency_hz,
            MAX(frequency) AS max_frequency_hz,
            AVG(power_factor) AS avg_power_factor,
            MIN(power_factor) AS min_power_factor,
            AVG(power_factor) - MIN(power_factor) AS power_factor_trend,
            CASE
                WHEN AVG(power_factor) < {thresholds.get("min_power_factor", 0.9)} THEN 'POOR_POWER_FACTOR'
                WHEN AVG(thd_voltage) > {thresholds.get("max_thd_voltage", 5.0)} THEN 'HIGH_THD'
                WHEN STDDEV_POP(avg_phase_voltage) / NULLIF(AVG(avg_phase_voltage), 0) > {thresholds.get("max_voltage_cv", 0.02)} THEN 'UNSTABLE_VOLTAGE'
                ELSE 'OK'
            END AS power_factor_quality_flag,
            AVG(thd_voltage) AS avg_thd_voltage,
            MAX(thd_voltage) AS max_thd_voltage,
            MAX(thd_voltage) - AVG(thd_voltage) AS thd_voltage_trend,
            AVG(thd_current) AS avg_thd_current,
            MAX(thd_current) AS max_thd_current,
            MAX(thd_current) - AVG(thd_current) AS thd_current_trend,
            SUM(CASE WHEN breaker_status THEN 1 ELSE 0 END) * 1.0 AS running_samples,
            COUNT(*) * 1.0 AS total_samples,
            MAX(CASE WHEN breaker_status THEN 1 ELSE 0 END) AS is_running,
            AVG(calc_reactive_power_kvar) / NULLIF(AVG(active_power_kw), 0) AS reactive_power_ratio,
            AVG(calc_apparent_power_kva) / NULLIF({settings.max_expected_power_kw}, 0) AS apparent_power_utilization,
            {settings.max_expected_power_kw} AS configured_max_power_kw
        FROM TABLE(
            TUMBLE(TABLE electrical_delta, DESCRIPTOR(event_time), {settings.window_interval_sql})
        )
        GROUP BY window_start, window_end, tag_id
        """
    )

    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW process_window AS
        SELECT
            window_start,
            window_end,
            tag_id,
            AVG(belt_speed) AS avg_belt_speed,
            MAX(belt_speed) AS max_belt_speed,
            AVG(pump_speed) AS avg_pump_speed,
            AVG(air_flow) AS avg_air_flow,
            AVG(air_pressure) AS avg_air_pressure,
            AVG(COALESCE(flow, instant_flow)) AS avg_flow,
            AVG(temperature) AS avg_temperature,
            AVG(agitator_speed) AS avg_agitator_speed,
            AVG(CASE WHEN status THEN 1.0 ELSE 0.0 END) * 100.0 AS running_percent
        FROM TABLE(
            TUMBLE(TABLE process_var_src, DESCRIPTOR(event_time), {settings.window_interval_sql})
        )
        GROUP BY window_start, window_end, tag_id
        """
    )

    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW water_window AS
        SELECT window_start, window_end, MAX(plant_id) AS plant_id, line_id,
            SUM(water_delta_m3) AS water_m3,
            SUM(water_delta_m3) * {settings.water_tariff_per_m3} AS water_cost
        FROM TABLE(TUMBLE(TABLE water_delta, DESCRIPTOR(event_time), {settings.window_interval_sql}))
        GROUP BY window_start, window_end, line_id
        """
    )

    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW steam_fuel_window AS
        SELECT window_start, window_end, MAX(plant_id) AS plant_id, line_id,
            SUM(steam_delta) AS steam_consumption,
            AVG(steam_pressure) AS avg_steam_pressure,
            AVG(steam_temperature) AS avg_steam_temperature,
            MAX(steam_flow_rate) AS max_steam_flow,
            SUM(fuel_delta) AS fuel_consumption,
            AVG(fuel_pressure) AS avg_fuel_pressure,
            AVG(fuel_temperature) AS avg_fuel_temperature,
            MAX(fuel_flow_rate) AS max_fuel_flow,
            SUM(fuel_delta) * {settings.fuel_price_per_unit} AS fuel_cost
        FROM TABLE(TUMBLE(TABLE steam_fuel_delta, DESCRIPTOR(event_time), {settings.window_interval_sql}))
        GROUP BY window_start, window_end, line_id
        """
    )

    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW rolling_peak AS
        SELECT window_end, tag_id, MAX(active_power_kw) AS rolling_15m_peak_kw
        FROM TABLE(HOP(TABLE electrical_delta, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, {settings.rolling_peak_interval_sql}))
        GROUP BY window_start, window_end, tag_id
        """
    )

    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW rolling_energy AS
        SELECT window_end, tag_id, SUM(energy_delta_kwh) AS rolling_60m_energy_kwh
        FROM TABLE(HOP(TABLE electrical_delta, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, {settings.rolling_energy_interval_sql}))
        GROUP BY window_start, window_end, tag_id
        """
    )


def start_inserts(t_env, settings: KpiSettings) -> list:
    """Start all continuous INSERT statements as one Flink job."""
    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql(
        f"""
        INSERT INTO equipment_kpis_sink
        SELECT
            e.window_start,
            e.window_end,
            e.tag_id,
            e.plant_id,
            e.line_id,
            e.area_id,
            e.energy_delta_kwh,
            e.total_energy_kwh,
            e.energy_cost,
            e.co2_kg,
            e.active_power_avg_kw,
            e.active_power_min_kw,
            e.active_power_max_kw,
            e.apparent_power_avg_kva,
            e.reactive_power_avg_kvar,
            e.peak_demand_kw,
            e.load_factor,
            e.avg_voltage_ln,
            e.voltage_stability_cv,
            e.voltage_unbalance_percent,
            e.avg_current_a,
            e.min_current_a,
            e.max_current_a,
            e.current_unbalance_percent,
            e.avg_frequency_hz,
            e.min_frequency_hz,
            e.max_frequency_hz,
            e.avg_power_factor,
            e.min_power_factor,
            e.power_factor_trend,
            e.power_factor_quality_flag,
            e.avg_thd_voltage,
            e.max_thd_voltage,
            e.thd_voltage_trend,
            e.avg_thd_current,
            e.max_thd_current,
            e.thd_current_trend,
            e.running_samples * {settings.window_seconds}.0 / NULLIF(e.total_samples, 0),
            (e.total_samples - e.running_samples) * {settings.window_seconds}.0 / NULLIF(e.total_samples, 0),
            e.running_samples / NULLIF(e.total_samples, 0) * 100.0,
            e.running_samples / NULLIF(e.total_samples, 0) * 100.0,
            e.peak_demand_kw / NULLIF(e.configured_max_power_kw, 0),
            e.reactive_power_ratio,
            e.apparent_power_utilization,
            rp.rolling_15m_peak_kw,
            re.rolling_60m_energy_kwh,
            GREATEST(0.0, 100.0 - COALESCE(e.voltage_unbalance_percent, 0.0) * 10.0 - COALESCE(e.voltage_stability_cv, 0.0) * 1000.0),
            GREATEST(0.0, 100.0 - (1.0 - COALESCE(e.avg_power_factor, 1.0)) * 100.0 - COALESCE(e.avg_thd_voltage, 0.0) * 2.0 - COALESCE(e.voltage_stability_cv, 0.0) * 1000.0),
            e.energy_delta_kwh,
            e.co2_kg,
            e.energy_cost,
            p.avg_belt_speed,
            p.max_belt_speed,
            p.avg_pump_speed,
            p.avg_air_flow,
            p.avg_air_pressure,
            p.avg_flow,
            p.avg_temperature,
            p.avg_agitator_speed,
            p.running_percent,
            GREATEST(0.0, 100.0 - COALESCE(e.avg_thd_voltage, 0.0) * 2.0 - (1.0 - COALESCE(e.avg_power_factor, 1.0)) * 100.0 - COALESCE(e.voltage_stability_cv, 0.0) * 1000.0 - CASE WHEN e.is_running = 1 THEN 0 ELSE 10 END)
        FROM equipment_window e
        LEFT JOIN rolling_peak rp ON e.tag_id = rp.tag_id AND e.window_end = rp.window_end
        LEFT JOIN rolling_energy re ON e.tag_id = re.tag_id AND e.window_end = re.window_end
        LEFT JOIN process_window p ON e.tag_id = p.tag_id AND e.window_end = p.window_end
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO area_kpis_sink
        SELECT window_start, window_end, area_id, MAX(plant_id), MAX(line_id),
            SUM(energy_delta_kwh), SUM(energy_cost), SUM(co2_kg),
            AVG(active_power_avg_kw), MAX(peak_demand_kw),
            SUM(active_power_avg_kw) / NULLIF(SUM(peak_demand_kw), 0),
            SUM(is_running), COUNT(*),
            SUM(running_samples) / NULLIF(SUM(total_samples), 0) * 100.0,
            CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)
        FROM equipment_window
        GROUP BY window_start, window_end, area_id
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO line_kpis_sink
        SELECT e.window_start, e.window_end, e.line_id, MAX(e.plant_id),
            SUM(e.energy_delta_kwh), SUM(e.energy_cost), SUM(e.co2_kg),
            AVG(e.active_power_avg_kw), MAX(e.peak_demand_kw),
            SUM(e.active_power_avg_kw) / NULLIF(SUM(e.peak_demand_kw), 0),
            MAX(w.water_m3), MAX(w.water_cost),
            MAX(s.steam_consumption), MAX(s.avg_steam_pressure), MAX(s.avg_steam_temperature), MAX(s.max_steam_flow),
            MAX(s.fuel_consumption), MAX(s.avg_fuel_pressure), MAX(s.avg_fuel_temperature), MAX(s.max_fuel_flow), MAX(s.fuel_cost)
        FROM equipment_window e
        LEFT JOIN water_window w ON e.line_id = w.line_id AND e.window_end = w.window_end
        LEFT JOIN steam_fuel_window s ON e.line_id = s.line_id AND e.window_end = s.window_end
        GROUP BY e.window_start, e.window_end, e.line_id
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO plant_kpis_sink
        SELECT window_start, window_end, plant_id,
            SUM(energy_delta_kwh), SUM(energy_cost), SUM(co2_kg),
            SUM(co2_kg), SUM(co2_kg),
            AVG(active_power_avg_kw), MAX(peak_demand_kw), MAX(peak_demand_kw), MAX(peak_demand_kw),
            SUM(active_power_avg_kw) / NULLIF(SUM(peak_demand_kw), 0),
            CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
            CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)
        FROM equipment_window
        GROUP BY window_start, window_end, plant_id
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO energy_consumption_kpi_sink
        SELECT window_end, window_start, window_end, 'EQUIPMENT', tag_id, plant_id, line_id, area_id, tag_id,
            energy_delta_kwh, total_energy_kwh, energy_cost, co2_kg
        FROM equipment_window
        UNION ALL
        SELECT window_end, window_start, window_end, 'AREA', CAST(area_id AS STRING), MAX(plant_id), MAX(line_id), area_id, CAST(NULL AS STRING),
            SUM(energy_delta_kwh), SUM(total_energy_kwh), SUM(energy_cost), SUM(co2_kg)
        FROM equipment_window GROUP BY window_start, window_end, area_id
        UNION ALL
        SELECT window_end, window_start, window_end, 'LINE', CAST(line_id AS STRING), MAX(plant_id), line_id, CAST(NULL AS INT), CAST(NULL AS STRING),
            SUM(energy_delta_kwh), SUM(total_energy_kwh), SUM(energy_cost), SUM(co2_kg)
        FROM equipment_window GROUP BY window_start, window_end, line_id
        UNION ALL
        SELECT window_end, window_start, window_end, 'PLANT', CAST(plant_id AS STRING), plant_id, CAST(NULL AS INT), CAST(NULL AS INT), CAST(NULL AS STRING),
            SUM(energy_delta_kwh), SUM(total_energy_kwh), SUM(energy_cost), SUM(co2_kg)
        FROM equipment_window GROUP BY window_start, window_end, plant_id
        """
    )
    return [statement_set.execute()]
