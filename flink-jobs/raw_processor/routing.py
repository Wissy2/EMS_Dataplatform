"""
routing.py
Routes NormalisedRecord to the correct sink via Flink OutputTags (side outputs).
One tag per destination table + one for DLQ errors.
"""
from pyflink.datastream import ProcessFunction
from pyflink.datastream.output_tag import OutputTag
from pyflink.common.typeinfo import Types
from models import NormalisedRecord, MessageType, ErrorRecord

# ── Output tags (one per destination table) ───────────────────────────────────
# All carry a serialised NormalisedRecord except TAG_ERROR
TAG_ELECTRICAL  = OutputTag("electrical",   Types.PICKLED_BYTE_ARRAY())
TAG_PROCESS_VAR = OutputTag("process_var",  Types.PICKLED_BYTE_ARRAY())
TAG_STEAM_FUEL  = OutputTag("steam_fuel",   Types.PICKLED_BYTE_ARRAY())
TAG_WATER       = OutputTag("water",        Types.PICKLED_BYTE_ARRAY())
TAG_ENERGY      = OutputTag("energy",       Types.PICKLED_BYTE_ARRAY())
TAG_ERROR       = OutputTag("error",        Types.PICKLED_BYTE_ARRAY())

# Main output = raw_measurements (ALL valid + invalid records)
# Side outputs = typed tables + DLQ

class RouterFunction(ProcessFunction):
    """
    Receives NormalisedRecord, emits it to:
      - main output  : always (raw_measurements truth table)
      - side output  : based on message_type (typed table)
      - TAG_ERROR    : if message_type == UNKNOWN (shouldn't happen after parsing)
    """
    def process_element(self, record: NormalisedRecord, ctx: ProcessFunction.Context):
        # Always emit to raw_measurements
        yield record

        # Route to typed table
        if record.message_type == MessageType.ELECTRICAL_PM:
            yield TAG_ELECTRICAL, record
        elif record.message_type == MessageType.PROCESS_VAR:
            yield TAG_PROCESS_VAR, record
        elif record.message_type == MessageType.STEAM_FUEL:
            yield TAG_STEAM_FUEL, record
        elif record.message_type == MessageType.WATER_AGG:
            yield TAG_WATER, record
        elif record.message_type == MessageType.ENERGY_AGG:
            yield TAG_ENERGY, record
        # UNKNOWN — flag it but don't crash; it already has validation flags set
        # It still went to raw_measurements above, just not to any typed table
