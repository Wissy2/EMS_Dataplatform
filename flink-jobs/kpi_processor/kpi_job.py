"""EMS Analytics/KPI Flink job.

Consumes normalized Kafka streams produced after the raw parsing layer and
writes layered KPI windows to TimescaleDB:

equipment -> area -> production line -> plant.
"""

from __future__ import annotations

import logging
import sys
import types

if "apache_beam" not in sys.modules:
    sys.modules["apache_beam"] = types.ModuleType("apache_beam")

from pyflink.common import Configuration
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

from aggregators.sql import register_sources, register_views, start_inserts
from config.settings import load_settings
from writers.timescale import register_kpi_sinks


log = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def main() -> None:
    configure_logging()
    settings = load_settings()

    log.info("Starting EMS Analytics/KPI Flink job")
    log.info("Kafka broker: %s", settings.kafka_broker)
    log.info("Window size: %s", settings.window_size)
    log.info("Timescale JDBC URL: %s", settings.timescale_jdbc_url)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(settings.flink_parallelism)
    env.enable_checkpointing(settings.checkpoint_interval_ms)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5_000)
    env.get_checkpoint_config().set_checkpoint_timeout(120_000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10_000))

    table_config = Configuration()
    table_config.set_string("table.local-time-zone", "UTC")
    environment_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(
        env,
        environment_settings=environment_settings,
    )
    t_env.get_config().add_configuration(table_config)

    register_sources(t_env, settings)
    register_views(t_env, settings)
    register_kpi_sinks(t_env, settings)

    handles = start_inserts(t_env, settings)
    log.info("Submitted %d continuous KPI sink statements", len(handles))

    for handle in handles:
        job_client = handle.get_job_client()
        if job_client is not None:
            log.info("KPI statement job id: %s", job_client.get_job_id())


if __name__ == "__main__":
    main()
