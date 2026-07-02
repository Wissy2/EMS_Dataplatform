"""
main.py
EMS Raw Measurements Processor — Job 1 (v2)
Updated to use ems.* schema with FK lookups via MetadataCache
(line_id/area_id/plant_id are INTEGER, equipment is tag_id VARCHAR).

Pipeline:
  Kafka topics (ems.L1.*)
      → KafkaEnvelope
      → parse_envelope()        [JSON parse + type detection + FK resolution]
      → validate_record()       [flag anomalies, never drop]
      → RouterFunction          [main output + 5 side outputs]
      → JDBC sinks              [raw_measurements + 5 typed tables]
      → DLQ Kafka sink          [ErrorRecord → ems.dlq]
"""

# ── IMPORTANT: must run before any pyflink import ─────────────────────────────
# Some PyFlink distributions bundle apache-beam for Python UDF type coercion.
# Beam's gcp.bigquery submodule does slow/blocking credential & network probing
# on import, which can hang the client for minutes or indefinitely with no
# network egress. We never use Beam — block it from loading.
import sys
import types

import logging

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
    ProcessFunction,
)
from pyflink.datastream.connectors.kafka import (
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema

import config
import utils
from metadata import CACHE
from kafka_source import build_kafka_source
from models import KafkaEnvelope, NormalisedRecord, ErrorRecord, MessageType
from parser import parse_envelope
from validators import validate_record
from routing import (
    RouterFunction,
    TAG_ELECTRICAL, TAG_PROCESS_VAR, TAG_STEAM_FUEL,
    TAG_WATER, TAG_ENERGY, TAG_ERROR,
)
from database import (
    raw_measurements_sink, RAW_TYPE,
    electrical_measurements_sink, ELECTRICAL_TYPE,
    process_variables_sink,
    steam_fuel_sink,
    water_consumption_sink,
    energy_consumption_sink,
)

log = logging.getLogger(__name__)


# ── Step 1: wrap raw string to KafkaEnvelope ──────────────────────────────────
class WrapEnvelope(ProcessFunction):
    def process_element(self, value, ctx):
        yield KafkaEnvelope(
            topic     = "",
            partition = -1,
            offset    = -1,
            kafka_ts  = 0,
            key       = None,
            value     = value.encode("utf-8") if isinstance(value, str) else value,
            headers   = {},
        )


# ── Step 2: parse + FK resolution ────────────────────────────────────────────
class ParseFunction(ProcessFunction):
    """
    Calls parse_envelope() with the global MetadataCache.
    Errors go to TAG_ERROR side output.
    """
    def process_element(self, envelope: KafkaEnvelope, ctx):
        record, error = parse_envelope(envelope, CACHE)
        if error is not None:
            log.warning(
                "Parse error [%s] offset=%d: %s",
                error.error_type, error.offset, error.error_message
            )
            ctx.output(TAG_ERROR, error)
        else:
            yield record


# ── Step 3: validate (inline map) ────────────────────────────────────────────
class ValidateFunction(ProcessFunction):
    def process_element(self, record: NormalisedRecord, ctx):
        yield validate_record(record)

from database import (
    raw_measurements_sink, RAW_TYPE,
    electrical_measurements_sink, ELECTRICAL_TYPE,
    process_variables_sink, PROCESS_VAR_TYPE,
    steam_fuel_sink, STEAM_FUEL_TYPE,
    water_consumption_sink, WATER_TYPE,
    energy_consumption_sink, ENERGY_TYPE,
    to_raw_row, to_electrical_row, to_process_var_row,
    to_steam_fuel_row, to_water_row, to_energy_row,
)
def main():
    utils.configure_logging()
    log.info("=" * 60)
    log.info("EMS Raw Measurements Processor starting")
    log.info("Kafka:       %s", config.KAFKA_BROKER)
    log.info("Topics:      %s", config.KAFKA_SOURCE_TOPICS)
    log.info("TimescaleDB: %s/%s", config.TIMESCALE_HOST, config.TIMESCALE_DB)
    log.info("Parallelism: %d", config.FLINK_PARALLELISM)
    log.info("=" * 60)

    # ── Load metadata BEFORE submitting the Flink job ─────────────────────────
    # This runs on the client (not inside TaskManagers) so one DB connection
    # resolves all device IDs into the in-memory CACHE singleton.
    log.info("Loading metadata from ems.equipment / areas / production_lines...")
    CACHE.load()
    log.info("Metadata loaded successfully")

    # ── Flink environment ─────────────────────────────────────────────────────
    #
    # IMPORTANT: JDBC connector classes (JdbcSink, JdbcExecutionOptions, etc.)
    # are resolved by the CLIENT-SIDE py4j gateway the moment Python code calls
    # JdbcExecutionOptions.builder() — this happens while building the job graph,
    # before env.execute() and before TaskManagers ever start. That means the
    # jars must be on the classpath of the JVM that's already running inside
    # `flink run`'s Python driver process, which is configured via the
    # `pipeline.jars` Configuration option set on the environment constructor,
    # NOT via env.add_jars() called afterward (which only affects the job
    # graph submitted to TaskManagers, too late for client-side class lookups).
    from pyflink.common import Configuration

    jar_paths = [
        "file:///opt/flink/lib/postgresql-42.6.0.jar",
        "file:///opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
    ]

    flink_config = Configuration()
    flink_config.set_string("pipeline.jars", ";".join(jar_paths))
    flink_config.set_string("pipeline.classpaths", ";".join(jar_paths))

    env = StreamExecutionEnvironment.get_execution_environment(flink_config)
    env.set_parallelism(config.FLINK_PARALLELISM)

    # Also register via add_jars for TaskManager-side job submission —
    # belt-and-braces, doesn't hurt to have both.
    env.add_jars(*jar_paths)

    # Exactly-once checkpointing
    env.enable_checkpointing(config.CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5_000)
    env.get_checkpoint_config().set_checkpoint_timeout(60_000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    # ── Kafka source ──────────────────────────────────────────────────────────
    kafka_source = build_kafka_source(config.KAFKA_SOURCE_TOPICS)
    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka EMS Raw Topics",
    )

    # ── Step 1: Wrap ──────────────────────────────────────────────────────────
    envelope_stream = (
        raw_stream
        .process(WrapEnvelope(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("Wrap → KafkaEnvelope")
    )

    # ── Step 2: Parse + resolve FKs ───────────────────────────────────────────
    parsed_stream = (
        envelope_stream
        .process(ParseFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("Parse JSON + resolve IDs")
    )

    # ── Step 3: Validate ──────────────────────────────────────────────────────
    validated_stream = (
        parsed_stream
        .process(ValidateFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("Validate")
    )

    # ── Step 4: Route ─────────────────────────────────────────────────────────
    routed_stream = (
        validated_stream
        .process(RouterFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("Route by message type")
    )
    
    #Step 5 : Sinks
    # 5a. ems.raw_measurements
    routed_stream \
        .map(to_raw_row, output_type=RAW_TYPE) \
        .add_sink(raw_measurements_sink()) \
        .name("→ ems.raw_measurements")

    # 5b. Typed side-output sinks
    routed_stream.get_side_output(TAG_ELECTRICAL) \
        .map(to_electrical_row, output_type=ELECTRICAL_TYPE) \
        .add_sink(electrical_measurements_sink()) \
        .name("→ ems.electrical_measurements")

    routed_stream.get_side_output(TAG_PROCESS_VAR) \
        .filter(lambda r: r.ids.tag_id is not None) \
        .map(to_process_var_row, output_type=PROCESS_VAR_TYPE) \
        .add_sink(process_variables_sink()) \
        .name("→ ems.process_variables")

    routed_stream.get_side_output(TAG_STEAM_FUEL) \
        .map(to_steam_fuel_row, output_type=STEAM_FUEL_TYPE) \
        .add_sink(steam_fuel_sink()) \
        .name("→ ems.steam_fuel_measurements")

    routed_stream.get_side_output(TAG_WATER) \
        .map(to_water_row, output_type=WATER_TYPE) \
        .add_sink(water_consumption_sink()) \
        .name("→ ems.water_consumption")

    routed_stream.get_side_output(TAG_ENERGY) \
        .map(to_energy_row, output_type=ENERGY_TYPE) \
        .add_sink(energy_consumption_sink()) \
        .name("→ ems.energy_consumption")


    # ── Execute ───────────────────────────────────────────────────────────────
    log.info("Submitting job to Flink cluster...")
    env.execute("EMS Raw Measurements Processor")


if __name__ == "__main__":
    main()
