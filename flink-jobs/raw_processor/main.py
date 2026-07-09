"""
main.py
EMS Raw Measurements Processor — Job 1 (v3)

Fixes applied vs the previous version:
  1. Real Kafka metadata (topic/partition/offset/headers) now flows through
     via kafka_source.build_kafka_envelope_stream(), instead of the previous
     WrapEnvelope stub which always produced topic="", headers={}. That stub
     silently broke message-type detection for steam/fuel, water, and energy
     records, and broke plant/line/area extraction for every record.
  2. The DLQ side output (TAG_ERROR) is now actually sunk to Kafka. Before,
     ErrorRecords were built and logged once, then discarded.
  3. The electrical_measurements sink now filters out records with an
     unresolved tag_id (mirroring the existing process_variables filter),
     since tag_id is NOT NULL in that table — previously an unresolved
     device would throw a JDBC constraint violation instead of just being
     visible (unrouted) in raw_measurements.
  4. Removed the duplicate/dead second `from database import (...)` block.

Pipeline:
  Kafka topics (ems.L1.*)  [Table API, real metadata]
      → KafkaEnvelope
      → parse_envelope()        [JSON parse + type detection + FK resolution]
      → validate_record()       [flag anomalies, never drop]
      → direct stream filters   [raw_measurements + 5 typed tables]
      → JDBC sinks
      → DLQ Kafka sink          [ErrorRecord → ems.dlq]
"""

# ── IMPORTANT: must run before any pyflink import ─────────────────────────────
# Some PyFlink distributions bundle apache-beam for Python UDF type coercion.
# Beam's gcp.bigquery submodule does slow/blocking credential & network probing
# on import, which can hang the client for minutes or indefinitely with no
# network egress. We never use Beam — block it from loading.
import sys
import types

# The comment above previously described this guard but never implemented it —
# sys/types were imported and left unused. Implementing it for real: register a
# stub "apache_beam" module before pyflink can try to import the real one, so
# any internal `import apache_beam...` becomes a no-op instead of triggering
# Beam's gcp.bigquery credential/network probing on import.
if "apache_beam" not in sys.modules:
    sys.modules["apache_beam"] = types.ModuleType("apache_beam")

import logging

from pyflink.common.typeinfo import Types
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
    ProcessFunction,
)
from pyflink.table import StreamTableEnvironment

import config
import utils
from metadata import CACHE
from kafka_source import (
    build_kafka_envelope_stream,
    build_kafka_dlq_sink,
    build_json_topic_sink,
    error_record_to_json,
    electrical_record_to_json,
    process_var_record_to_json,
    steam_fuel_record_to_json,
    water_record_to_json,
)
from models import KafkaEnvelope, NormalisedRecord, ErrorRecord, MessageType
from parser import parse_envelope
from validators import validate_record
from routing import TAG_ERROR
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

log = logging.getLogger(__name__)


# ── Step 1: parse + FK resolution ────────────────────────────────────────────
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


# ── Step 2: validate (inline map) ────────────────────────────────────────────
class ValidateFunction(ProcessFunction):
    def process_element(self, record: NormalisedRecord, ctx):
        yield validate_record(record)


def is_electrical_record(record: NormalisedRecord) -> bool:
    return record.message_type == MessageType.ELECTRICAL_PM


def is_process_var_record(record: NormalisedRecord) -> bool:
    return record.message_type == MessageType.PROCESS_VAR


def is_steam_fuel_record(record: NormalisedRecord) -> bool:
    return record.message_type == MessageType.STEAM_FUEL


def is_water_record(record: NormalisedRecord) -> bool:
    return record.message_type == MessageType.WATER_AGG


def is_energy_record(record: NormalisedRecord) -> bool:
    return record.message_type == MessageType.ENERGY_AGG


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
    # NOTE: earlier versions of this file explicitly registered these 3 jars
    # via a Configuration object's pipeline.jars/pipeline.classpaths, plus a
    # separate env.add_jars() call. That combination caused:
    #   "IllegalStateException: different set of library BLOBs" on any task
    #   retry, because the two mechanisms didn't replace each other's value,
    #   they combined, doubling the effective jar list on the second
    #   registration.
    #
    # Removing add_jars() alone did not fix it — the same doubled list kept
    # appearing, which means something outside this script (cluster startup
    # config, the submission command, or flink-conf.yaml) is independently
    # setting pipeline.jars/pipeline.classpaths to the same 3 jars, and it was
    # colliding with this script's own registration of the identical jars.
    #
    # The actual fix: don't register them here at all. Jars placed in
    # /opt/flink/lib/ are automatically on the classpath of every Flink JVM
    # (JobManager, TaskManager, and the `flink run` driver process) — that is
    # the standard purpose of that directory in the Flink Docker image. No
    # explicit pipeline.jars/pipeline.classpaths/add_jars() registration is
    # needed for jars that already live there, for either client-side class
    # resolution (JdbcSink/JdbcExecutionOptions, resolved when this script
    # calls their builders) or TaskManager-side execution.
    #
    # If postgresql-42.6.0.jar / flink-connector-jdbc-3.1.2-1.18.jar /
    # flink-sql-connector-kafka-3.1.0-1.18.jar are ever moved out of
    # /opt/flink/lib/ (e.g. into a job-specific directory), this will need an
    # explicit registration again — but only ONE mechanism, not two.
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.FLINK_PARALLELISM)

    # Exactly-once checkpointing
    env.enable_checkpointing(config.CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5_000)
    env.get_checkpoint_config().set_checkpoint_timeout(60_000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    # ── Kafka source (Table API, real metadata) ───────────────────────────────
    # See kafka_source.py for why this must be Table API rather than the
    # DataStream KafkaSource: only Table API METADATA VIRTUAL columns expose
    # per-record topic/partition/offset/headers to Python code in PyFlink.
    t_env = StreamTableEnvironment.create(env)
    envelope_stream = build_kafka_envelope_stream(env, t_env, config.KAFKA_SOURCE_TOPICS)

    # ── Step 1: Parse + resolve FKs ───────────────────────────────────────────
    parsed_stream = (
        envelope_stream
        .process(ParseFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("Parse JSON + resolve IDs")
    )

    # ── Step 2: Validate ──────────────────────────────────────────────────────
    validated_stream = (
        parsed_stream
        .process(ValidateFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("Validate")
    )

    # ── Step 4: Sinks ─────────────────────────────────────────────────────────
    # 4a. ems.raw_measurements — every record, valid or not
    validated_stream \
        .map(to_raw_row, output_type=RAW_TYPE) \
        .add_sink(raw_measurements_sink()) \
        .name("→ ems.raw_measurements")

    # 4b. Typed sinks branch from the validated stream. The raw sink already
    # proves this stream carries parsed records; filtering here avoids relying
    # on PyFlink side-output serialization for normal table routing.
    validated_stream \
        .filter(is_electrical_record) \
        .filter(lambda r: r.ids.tag_id is not None) \
        .map(to_electrical_row, output_type=ELECTRICAL_TYPE) \
        .add_sink(electrical_measurements_sink()) \
        .name("→ ems.electrical_measurements")

    validated_stream \
        .filter(is_electrical_record) \
        .filter(lambda r: r.ids.tag_id is not None) \
        .map(electrical_record_to_json, output_type=Types.STRING()) \
        .sink_to(build_json_topic_sink(config.KAFKA_NORMALIZED_ELECTRICAL_TOPIC)) \
        .name("→ ems.normalized.electrical_measurements")

    validated_stream \
        .filter(is_process_var_record) \
        .filter(lambda r: r.ids.tag_id is not None) \
        .map(to_process_var_row, output_type=PROCESS_VAR_TYPE) \
        .add_sink(process_variables_sink()) \
        .name("→ ems.process_variables")

    validated_stream \
        .filter(is_process_var_record) \
        .filter(lambda r: r.ids.tag_id is not None) \
        .map(process_var_record_to_json, output_type=Types.STRING()) \
        .sink_to(build_json_topic_sink(config.KAFKA_NORMALIZED_PROCESS_VAR_TOPIC)) \
        .name("→ ems.normalized.process_variables")

    validated_stream \
        .filter(is_steam_fuel_record) \
        .map(to_steam_fuel_row, output_type=STEAM_FUEL_TYPE) \
        .add_sink(steam_fuel_sink()) \
        .name("→ ems.steam_fuel_measurements")

    validated_stream \
        .filter(is_steam_fuel_record) \
        .map(steam_fuel_record_to_json, output_type=Types.STRING()) \
        .sink_to(build_json_topic_sink(config.KAFKA_NORMALIZED_STEAM_FUEL_TOPIC)) \
        .name("→ ems.normalized.steam_fuel_measurements")

    validated_stream \
        .filter(is_water_record) \
        .map(to_water_row, output_type=WATER_TYPE) \
        .add_sink(water_consumption_sink()) \
        .name("→ ems.water_consumption")

    validated_stream \
        .filter(is_water_record) \
        .map(water_record_to_json, output_type=Types.STRING()) \
        .sink_to(build_json_topic_sink(config.KAFKA_NORMALIZED_WATER_TOPIC)) \
        .name("→ ems.normalized.water_consumption")

    validated_stream \
        .filter(is_energy_record) \
        .map(to_energy_row, output_type=ENERGY_TYPE) \
        .add_sink(energy_consumption_sink()) \
        .name("→ ems.energy_consumption")

    # 4c. DLQ — malformed records that failed parse_envelope()
    parsed_stream.get_side_output(TAG_ERROR) \
        .map(error_record_to_json, output_type=Types.STRING()) \
        .sink_to(build_kafka_dlq_sink()) \
        .name("→ ems.dlq")

    # ── Execute ───────────────────────────────────────────────────────────────
    log.info("Submitting job to Flink cluster...")
    env.execute("EMS Raw Measurements Processor")


if __name__ == "__main__":
    main()
