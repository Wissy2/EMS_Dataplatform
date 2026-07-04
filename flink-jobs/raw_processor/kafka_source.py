"""
kafka_source.py
Builds the real Kafka ingestion path using the Table API's Kafka connector.

WHY TABLE API AND NOT THE DATASTREAM KafkaSource:
The DataStream KafkaSource + SimpleStringSchema combination (used in the
previous version of this file) cannot expose per-record metadata — topic,
partition, offset, headers — to Python user code. That is a real PyFlink
limitation, not an oversight. The previous DeserialiseToEnvelope always
built envelopes with topic="", partition=-1, offset=-1, headers={}, which
silently broke:
  - message-type detection for steam/fuel, water, and energy records
    (parser._detect_type() keys off the real Kafka topic string)
  - mqtt_topic/plant/line/area extraction (parser reads the "mqtt-topic"
    Kafka header, which was never populated)

The bridge (mqtt-kafka-bridge.js) already attaches everything we need as
Kafka message headers (`mqtt-topic`, `source-line`, `source-area`,
`payload-type`) and names Kafka topics in a pattern parser.py already
expects (e.g. ems.L1.water_consumption, ems.L1.utilities.steam_fuel).
The only thing missing was actually reading that metadata off the wire.
The Table API's METADATA VIRTUAL columns are the supported way to do that
in PyFlink, so we read via a Table source and convert to a DataStream.
"""

import json
import logging
from typing import List

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.connectors.kafka import (
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema
from pyflink.table import StreamTableEnvironment

import config
from models import KafkaEnvelope, ErrorRecord

log = logging.getLogger(__name__)

# Internal name for the metadata-enabled Kafka source table.
_SOURCE_TABLE = "ems_kafka_raw_source"


def _build_source_ddl(topics: List[str]) -> str:
    """
    DDL for a Kafka source table exposing topic / partition / offset /
    headers / timestamp as METADATA VIRTUAL columns, plus the raw message
    body decoded as a single STRING column via 'format' = 'raw'.

    Column order below is load-bearing: RowToEnvelope.map() indexes into
    the Row positionally in this exact order.
    """
    topic_list = ";".join(topics)
    startup_mode = (
        "earliest-offset" if config.KAFKA_START_OFFSET == "earliest" else "latest-offset"
    )
    return f"""
        CREATE TABLE {_SOURCE_TABLE} (
            `mqtt_kafka_topic` STRING METADATA FROM 'topic' VIRTUAL,
            `kafka_partition`  INT METADATA FROM 'partition' VIRTUAL,
            `kafka_offset`     BIGINT METADATA FROM 'offset' VIRTUAL,
            `kafka_headers`    MAP<STRING, BYTES> METADATA FROM 'headers' VIRTUAL,
            `kafka_timestamp`  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
            `payload_value`    STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic_list}',
            'properties.bootstrap.servers' = '{config.KAFKA_BROKER}',
            'properties.group.id' = '{config.KAFKA_GROUP_ID}',
            'scan.startup.mode' = '{startup_mode}',
            'format' = 'raw'
        )
    """


class RowToEnvelope(MapFunction):
    """
    Converts a Row from the metadata-enabled Kafka source table into a
    KafkaEnvelope, decoding header byte-values (Kafka headers are raw
    bytes on the wire) into UTF-8 strings so parser.py can read them
    with plain dict .get() calls.

    Positional column order matches _build_source_ddl() exactly:
      0: mqtt_kafka_topic (STRING)         -> real Kafka topic, e.g. "ems.L1.extraction.pm"
      1: kafka_partition  (INT)
      2: kafka_offset     (BIGINT)
      3: kafka_headers    (MAP<STRING, BYTES>) -> includes "mqtt-topic", "source-line",
                                                   "source-area", "payload-type"
      4: kafka_timestamp  (TIMESTAMP_LTZ(3))
      5: payload_value    (STRING)         -> the raw JSON message body
    """

    def map(self, row: Row) -> KafkaEnvelope:
        topic       = row[0] or ""
        partition   = row[1] if row[1] is not None else -1
        offset      = row[2] if row[2] is not None else -1
        raw_headers = row[3] or {}
        value       = row[5] or ""

        headers = {}
        for k, v in raw_headers.items():
            if isinstance(v, (bytes, bytearray)):
                headers[k] = v.decode("utf-8", errors="replace")
            elif v is not None:
                headers[k] = str(v)

        ts_val = row[4]
        try:
            kafka_ts = int(ts_val.timestamp() * 1000) if hasattr(ts_val, "timestamp") else int(ts_val or 0)
        except (TypeError, ValueError):
            kafka_ts = 0

        return KafkaEnvelope(
            topic     = topic,
            partition = int(partition),
            offset    = int(offset),
            kafka_ts  = kafka_ts,
            key       = headers.get("mqtt-topic"),   # informational; not used for dedup
            value     = value.encode("utf-8") if isinstance(value, str) else value,
            headers   = headers,
        )


def build_kafka_envelope_stream(
    env: StreamExecutionEnvironment,
    t_env: StreamTableEnvironment,
    topics: List[str],
) -> DataStream:
    """
    Registers the metadata-enabled Kafka source table and returns a
    DataStream[KafkaEnvelope] (pickled) with real topic/partition/offset/
    headers populated from the wire.
    """
    t_env.execute_sql(_build_source_ddl(topics))
    table = t_env.from_path(_SOURCE_TABLE)
    row_stream = t_env.to_data_stream(table)
    return (
        row_stream
        .map(RowToEnvelope(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("Kafka → KafkaEnvelope (real metadata)")
    )


# ── DLQ sink ────────────────────────────────────────────────────────────────
# Plain DataStream KafkaSink — no per-record metadata needed for producing,
# so the DataStream connector (not Table API) is the right tool here.

def error_record_to_json(err: ErrorRecord) -> str:
    """Serialise an ErrorRecord to a JSON string for the DLQ topic."""
    return json.dumps({
        "kafka_topic":   err.kafka_topic,
        "partition":     err.partition,
        "offset":        err.offset,
        "error_type":    err.error_type,
        "error_message": err.error_message,
        "raw_payload":   err.raw_payload,
        "processing_ts": err.processing_ts.isoformat(),
        "mqtt_topic":    err.mqtt_topic,
    })


def build_kafka_dlq_sink() -> KafkaSink:
    """KafkaSink that writes ErrorRecord (as JSON) to the configured DLQ topic."""
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(config.KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(config.KAFKA_DLQ_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
