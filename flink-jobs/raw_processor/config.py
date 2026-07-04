"""
config.py
All configuration is read from environment variables.
Never hardcode connection strings or topic names here.
"""

import os

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER        = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_GROUP_ID      = os.environ.get("KAFKA_GROUP_ID", "ems-raw-processor-v1")
KAFKA_START_OFFSET  = os.environ.get("KAFKA_START_OFFSET", "earliest")  # earliest | latest

# Topics to consume (all ems.L* except dlq)
KAFKA_SOURCE_TOPICS = [
    "ems.L1.extraction.pm",
    "ems.L1.extraction.pv",
    "ems.L1.washing.pm",
    "ems.L1.washing.pv",
    "ems.L1.flotation.pm",
    "ems.L1.flotation.pv",
    "ems.L1.utilities.pm",
    "ems.L1.utilities.pv",
    "ems.L1.utilities.steam_fuel",
    "ems.L1.storage_handling.pm",
    "ems.L1.storage_handling.pv",
    "ems.L1.water_consumption",
    "ems.L1.energy_consumption",
]

# Dead-letter topic for unparseable messages
KAFKA_DLQ_TOPIC     = os.environ.get("KAFKA_DLQ_TOPIC", "ems.dlq")

# ── TimescaleDB ───────────────────────────────────────────────────────────────
TIMESCALE_HOST      = os.environ.get("TIMESCALE_HOST", "timescaledb")
TIMESCALE_PORT      = int(os.environ.get("TIMESCALE_PORT", "5432"))
TIMESCALE_DB        = os.environ.get("TIMESCALE_DB", "ems_db")
TIMESCALE_USER      = os.environ.get("TIMESCALE_USER", "ems_user")
TIMESCALE_PASSWORD  = os.environ.get("TIMESCALE_PASSWORD", "ems_password")
TIMESCALE_JDBC_URL  = (
    f"jdbc:postgresql://{TIMESCALE_HOST}:{TIMESCALE_PORT}/{TIMESCALE_DB}"
)

# ── Flink job ─────────────────────────────────────────────────────────────────
FLINK_PARALLELISM       = int(os.environ.get("FLINK_PARALLELISM", "2"))
CHECKPOINT_INTERVAL_MS  = int(os.environ.get("CHECKPOINT_INTERVAL_MS", "30000"))
CHECKPOINT_DIR          = os.environ.get("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints")

# ── Validation thresholds ─────────────────────────────────────────────────────
MAX_FREQUENCY_HZ        = float(os.environ.get("MAX_FREQUENCY_HZ", "70.0"))
MAX_FUTURE_TIMESTAMP_S  = int(os.environ.get("MAX_FUTURE_TIMESTAMP_S", "300"))   # 5 min

# ── JDBC sink batching ────────────────────────────────────────────────────────
JDBC_BATCH_SIZE         = int(os.environ.get("JDBC_BATCH_SIZE", "500"))
JDBC_BATCH_INTERVAL_MS  = int(os.environ.get("JDBC_BATCH_INTERVAL_MS", "2000"))
JDBC_MAX_RETRIES        = int(os.environ.get("JDBC_MAX_RETRIES", "3"))

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL               = os.environ.get("LOG_LEVEL", "INFO")
