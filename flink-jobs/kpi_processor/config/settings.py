"""Configuration for the KPI job.

All business values live in config/kpi_config.json and can be overridden by
environment variables for deployments.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Dict


DEFAULT_CONFIG_PATH = Path(__file__).with_name("kpi_config.json")


@dataclass(frozen=True)
class KpiSettings:
    kafka_broker: str
    kafka_group_id: str
    kafka_start_offset: str
    timescale_jdbc_url: str
    timescale_user: str
    timescale_password: str
    flink_parallelism: int
    checkpoint_interval_ms: int
    jdbc_batch_size: int
    jdbc_batch_interval_ms: int
    jdbc_max_retries: int
    window_size: str
    window_seconds: int
    window_interval_sql: str
    rolling_peak_window: str
    rolling_peak_interval_sql: str
    rolling_energy_window: str
    rolling_energy_interval_sql: str
    electricity_tariff_per_kwh: float
    fuel_price_per_unit: float
    water_tariff_per_m3: float
    co2_emission_factor_kg_per_kwh: float
    max_expected_power_kw: float
    alarm_thresholds: Dict[str, float]
    topics: Dict[str, str]


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _float_env(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


def _int_env(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _parse_interval_parts(value: str) -> tuple[int, str]:
    parts = value.strip().split()
    if len(parts) != 2:
        raise ValueError(f"Unsupported interval format: {value!r}. Expected '<number> <unit>'.")
    return int(parts[0]), parts[1].lower()


def _parse_interval_seconds(value: str) -> int:
    amount, unit = _parse_interval_parts(value)
    if unit.startswith("second"):
        return amount
    if unit.startswith("minute"):
        return amount * 60
    if unit.startswith("hour"):
        return amount * 3600
    if unit.startswith("day"):
        return amount * 86400
    raise ValueError(f"Unsupported interval unit in {value!r}.")


def _flink_interval_sql(value: str) -> str:
    amount, unit = _parse_interval_parts(value)
    if unit.startswith("second"):
        normalized_unit = "SECOND"
    elif unit.startswith("minute"):
        normalized_unit = "MINUTE"
    elif unit.startswith("hour"):
        normalized_unit = "HOUR"
    elif unit.startswith("day"):
        normalized_unit = "DAY"
    else:
        raise ValueError(f"Unsupported interval unit in {value!r}.")
    return f"INTERVAL '{amount}' {normalized_unit}"


def load_settings() -> KpiSettings:
    config_path = Path(os.environ.get("KPI_CONFIG_PATH", str(DEFAULT_CONFIG_PATH)))
    data = _load_json(config_path)
    thresholds = dict(data.get("alarm_thresholds", {}))
    topics = dict(data.get("topics", {}))

    timescale_host = os.environ.get("TIMESCALE_HOST", "timescaledb")
    timescale_port = _int_env("TIMESCALE_PORT", 5432)
    timescale_db = os.environ.get("TIMESCALE_DB", "ems_db")

    window_size = os.environ.get("KPI_WINDOW_SIZE", data["window_size"])
    rolling_peak_window = os.environ.get("KPI_ROLLING_PEAK_WINDOW", data["rolling_peak_window"])
    rolling_energy_window = os.environ.get("KPI_ROLLING_ENERGY_WINDOW", data["rolling_energy_window"])

    return KpiSettings(
        kafka_broker=os.environ.get("KAFKA_BROKER", "kafka:9092"),
        kafka_group_id=os.environ.get("KPI_KAFKA_GROUP_ID", "ems-kpi-processor-v1"),
        kafka_start_offset=os.environ.get("KAFKA_START_OFFSET", "earliest"),
        timescale_jdbc_url=os.environ.get(
            "TIMESCALE_JDBC_URL",
            f"jdbc:postgresql://{timescale_host}:{timescale_port}/{timescale_db}",
        ),
        timescale_user=os.environ.get("TIMESCALE_USER", "ems_user"),
        timescale_password=os.environ.get("TIMESCALE_PASSWORD", "ems_password"),
        flink_parallelism=_int_env("FLINK_PARALLELISM", 2),
        checkpoint_interval_ms=_int_env("CHECKPOINT_INTERVAL_MS", 30000),
        jdbc_batch_size=_int_env("JDBC_BATCH_SIZE", 500),
        jdbc_batch_interval_ms=_int_env("JDBC_BATCH_INTERVAL_MS", 2000),
        jdbc_max_retries=_int_env("JDBC_MAX_RETRIES", 3),
        window_size=window_size,
        window_seconds=_int_env("KPI_WINDOW_SECONDS", _parse_interval_seconds(window_size)),
        window_interval_sql=_flink_interval_sql(window_size),
        rolling_peak_window=rolling_peak_window,
        rolling_peak_interval_sql=_flink_interval_sql(rolling_peak_window),
        rolling_energy_window=rolling_energy_window,
        rolling_energy_interval_sql=_flink_interval_sql(rolling_energy_window),
        electricity_tariff_per_kwh=_float_env(
            "ELECTRICITY_TARIFF_PER_KWH", float(data["electricity_tariff_per_kwh"])
        ),
        fuel_price_per_unit=_float_env("FUEL_PRICE_PER_UNIT", float(data["fuel_price_per_unit"])),
        water_tariff_per_m3=_float_env("WATER_TARIFF_PER_M3", float(data["water_tariff_per_m3"])),
        co2_emission_factor_kg_per_kwh=_float_env(
            "CO2_EMISSION_FACTOR_KG_PER_KWH",
            float(data["co2_emission_factor_kg_per_kwh"]),
        ),
        max_expected_power_kw=_float_env("MAX_EXPECTED_POWER_KW", float(data["max_expected_power_kw"])),
        alarm_thresholds=thresholds,
        topics=topics,
    )
