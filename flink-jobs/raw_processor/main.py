"""
main.py
EMS Raw Measurements Processor — Job 1
"""

import logging

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, ProcessFunction
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Configuration

import config
import utils
from kafka_source import build_kafka_source
from models import KafkaEnvelope, NormalisedRecord
from parser import parse_envelope
from validators import validate_record
from routing import RouterFunction, TAG_ELECTRICAL, TAG_PROCESS_VAR, TAG_STEAM_FUEL, TAG_WATER, TAG_ENERGY, TAG_ERROR
from database import (
    raw_measurements_sink, to_raw_row, RAW_TYPES,
    electrical_measurements_sink, to_electrical_row, ELECTRICAL_TYPES,
    process_variables_sink, to_process_var_row, PROCESS_VAR_TYPES,
    steam_fuel_sink, to_steam_fuel_row, STEAM_FUEL_TYPES,
    water_consumption_sink, to_water_row, WATER_TYPES,
    energy_consumption_sink, to_energy_row, ENERGY_TYPES
)

log = logging.getLogger(__name__)


class MetadataEnricher(MapFunction):
    """
    Looks up equipment_id, line_id, and area_id based on device_id or path.
    In production, query the DB inside `open()` and store results in `self.cache`.
    """
    def open(self, runtime_context):
        # TODO: Implement actual JDBC connection here using config parameters 
        # to fetch `ems.equipment`, `ems.areas`, and `ems.production_lines`.
        # Example cache structure populated by query:
        self.device_cache = {
            # "DEVICE_123": {"equipment_id": 1, "area_id": 1, "line_id": 1}
        }
        self.line_cache = {
            # "Line-1": {"line_id": 1}
        }

    def map(self, record: NormalisedRecord) -> NormalisedRecord:
        # Match against equipment
        if record.device_id and record.device_id in self.device_cache:
            meta = self.device_cache[record.device_id]
            record.equipment_id = meta.get("equipment_id")
            record.area_id = meta.get("area_id")
            record.line_id = meta.get("line_id")
        
        # Fallbacks for aggregate metrics that only have lines (Water/Steam/Energy)
        if record.line and not record.line_id and record.line in self.line_cache:
            record.line_id = self.line_cache[record.line].get("line_id")
            
        return record


class ParseFunction(ProcessFunction):
    def process_element(self, envelope: KafkaEnvelope, ctx):
        record, error = parse_envelope(envelope)
        if error is not None:
            log.warning(
                "Parse error offset=%d type=%s: %s",
                envelope.offset, error.error_type, error.error_message
            )
            ctx.output(TAG_ERROR, error)
        else:
            yield record


def main():
    utils.configure_logging()
    log.info("Starting EMS Raw Measurements Processor (Normalized DB)")
    
    # 1. Inject JARs into a Configuration object first (Renamed to flink_config!)
    flink_config = Configuration()
    flink_config.set_string(
        "pipeline.jars", 
        "file:///opt/flink/jobs/postgresql-42.6.0.jar;"
        "file:///opt/flink/jobs/flink-connector-jdbc-3.1.2-1.18.jar;"
        "file:///opt/flink/jobs/flink-sql-connector-kafka-3.1.0-1.18.jar;"
        "file:///opt/flink/opt/flink-python-1.18.0.jar"
    )

    # 2. Pass the flink_config to the environment builder
    env = StreamExecutionEnvironment.get_execution_environment(flink_config)
    
    # Now this will correctly use your config.py module again!
    env.set_parallelism(config.FLINK_PARALLELISM)

    env.enable_checkpointing(config.CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)


    # ── Step 0: Kafka Source ──────────────────────────────────────────────────
    kafka_source = build_kafka_source(config.KAFKA_SOURCE_TOPICS)

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka EMS Raw Topics",
    )

    # ── Step 1: Wrap raw string → KafkaEnvelope ───────────────────────────────
    envelope_stream = raw_stream.map(
        lambda value: KafkaEnvelope(
            topic     = "", 
            partition = -1,
            offset    = -1,
            kafka_ts  = 0,
            key       = None,
            value     = value.encode("utf-8") if isinstance(value, str) else value,
            headers   = {}, 
        ),
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).name("Wrap to KafkaEnvelope")

    # ── Step 2: Parse JSON ────────────────────────────────────────────────────
    parsed_stream = envelope_stream.process(
        ParseFunction(),
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).name("Parse JSON")

    # ── Step 3: Validate ──────────────────────────────────────────────────────
    validated_stream = parsed_stream.map(
        validate_record,
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).name("Validate records")

    # ── Step 3.5: Enrich with Database IDs ────────────────────────────────────
    enriched_stream = validated_stream.map(
        MetadataEnricher(),
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).name("Resolve Metadata IDs")

    # ── Step 4: Route to typed side outputs ───────────────────────────────────
    routed_stream = enriched_stream.process(
        RouterFunction(),
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).name("Route by message type")

# ── Step 5: Sinks ─────────────────────────────────────────────────────────

    routed_stream \
        .map(to_raw_row, output_type=RAW_TYPES) \
        .add_sink(raw_measurements_sink()) \
        .name("Sink → ems.raw_measurements")

    routed_stream.get_side_output(TAG_ELECTRICAL) \
        .map(to_electrical_row, output_type=ELECTRICAL_TYPES) \
        .add_sink(electrical_measurements_sink()) \
        .name("Sink → ems.electrical_measurements")

    routed_stream.get_side_output(TAG_PROCESS_VAR) \
        .map(to_process_var_row, output_type=PROCESS_VAR_TYPES) \
        .add_sink(process_variables_sink()) \
        .name("Sink → ems.process_variables")

    routed_stream.get_side_output(TAG_STEAM_FUEL) \
        .map(to_steam_fuel_row, output_type=STEAM_FUEL_TYPES) \
        .add_sink(steam_fuel_sink()) \
        .name("Sink → ems.steam_fuel_measurements")

    routed_stream.get_side_output(TAG_WATER) \
        .map(to_water_row, output_type=WATER_TYPES) \
        .add_sink(water_consumption_sink()) \
        .name("Sink → ems.water_consumption")

    routed_stream.get_side_output(TAG_ENERGY) \
        .map(to_energy_row, output_type=ENERGY_TYPES) \
        .add_sink(energy_consumption_sink()) \
        .name("Sink → ems.energy_consumption")

    # DLQ Setup
    error_stream = parsed_stream.get_side_output(TAG_ERROR)
    dlq_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(config.KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(config.KAFKA_DLQ_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    error_stream \
        .map(lambda e: f'{{"error_type":"{e.error_type}","topic":"{e.kafka_topic}","offset":{e.offset},"msg":"{e.error_message}"}}') \
        .sink_to(dlq_sink) \
        .name("Sink → ems.dlq")

    env.execute("EMS Raw Measurements Processor")


if __name__ == "__main__":
    main()
