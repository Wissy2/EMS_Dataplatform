"""
utils.py
Logging configuration and Flink metric accumulator helpers.
"""

import logging
import sys
from datetime import datetime, timezone

import config


def configure_logging() -> None:
    """Configure root logger for the Flink job."""
    level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    fmt   = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    logging.basicConfig(stream=sys.stdout, level=level, format=fmt)
    # Quiet noisy libraries
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyflink").setLevel(logging.WARNING)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── Flink metric accumulators ─────────────────────────────────────────────────
# These are registered on the RuntimeContext and exposed via the Flink Web UI
# and (optionally) Prometheus reporter.

METRIC_RECORDS_CONSUMED   = "ems.records_consumed"
METRIC_RECORDS_INSERTED   = "ems.records_inserted"
METRIC_VALIDATION_FAILED  = "ems.validation_failed"
METRIC_JSON_PARSE_ERRORS  = "ems.json_parse_errors"
METRIC_DB_FAILURES        = "ems.db_failures"
METRIC_PROCESSING_LATENCY = "ems.processing_latency_ms"
