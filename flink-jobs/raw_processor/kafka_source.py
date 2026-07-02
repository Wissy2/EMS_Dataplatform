"""
kafka_source.py
Flink KafkaSource configuration.
Reads from all ems.L* topics, deserialises to KafkaEnvelope.
"""

import logging
from typing import List

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction

import config
from models import KafkaEnvelope

log = logging.getLogger(__name__)


class DeserialiseToEnvelope(MapFunction):
    """
    Wraps the raw Kafka value string into a KafkaEnvelope.
    Note: PyFlink's KafkaSource with SimpleStringSchema doesn't expose
    headers directly — we read them from the Kafka consumer record
    via a custom deserialiser (see below).
    For simplicity we use the value-only source and rely on the
    mqtt-topic header being embedded in the key or recovered from
    the topic path; a full deserialiser is shown in the comment below.
    """

    def map(self, value: str):
        # In production replace this with KafkaRecordDeserialiser
        # to get full envelope including headers.
        # This stub creates an envelope with empty headers.
        return KafkaEnvelope(
            topic     = "",   # filled by KafkaRecordDeserialiser
            partition = -1,
            offset    = -1,
            kafka_ts  = 0,
            key       = None,
            value     = value.encode("utf-8") if isinstance(value, str) else value,
            headers   = {},
        )


def build_kafka_source(topics: List[str]) -> KafkaSource:
    """Build a KafkaSource that reads all EMS raw topics."""
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(config.KAFKA_BROKER)
        .set_topics(*topics)
        .set_group_id(config.KAFKA_GROUP_ID)
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
            if config.KAFKA_START_OFFSET == "earliest"
            else KafkaOffsetsInitializer.latest()
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_kafka_dlq_producer_props() -> dict:
    """Properties for the KafkaSink writing to the DLQ topic."""
    return {
        "bootstrap.servers":           config.KAFKA_BROKER,
        "transaction.timeout.ms":      "60000",
        "enable.idempotence":          "true",
    }
