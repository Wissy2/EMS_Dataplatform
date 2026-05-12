import json
import os
from datetime import datetime
from pyflink.common import WatermarkStrategy, Types, Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, WindowAssigner
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction

# ── Constants ────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC  = "ems.meters.1"
STATS_TOPIC  = "ems.energy.stats"

# CO2 Factors (kg CO2 per kWh) - Using Morocco Avg as a base
CO2_FACTOR_ELEC = 0.52 # Adjusted for local context (ONEE)

class EnergyStatsProcessFunction(ProcessWindowFunction):
    """
    Processes a 1-hour window to calculate:
    - Total kWh consumed in that hour
    - CO2 emissions
    - Specific Energy Consumption (if production data is available)
    """
    def process(self, key, context, elements):
        # elements = all messages for this device in the last hour
        device_id = key
        
        # Extract energy values and production counts
        energy_values = [msg.get("total_active_energy") for msg in elements if "total_active_energy" in msg]
        production_counts = [msg.get("units_produced") for msg in elements if "units_produced" in msg]

        if not energy_values:
            return

        # Consumption = Max - Min in this window
        consumption_kwh = max(energy_values) - min(energy_values)
        co2_emissions = consumption_kwh * CO2_FACTOR_ELEC
        
        # SEC Calculation
        total_units = sum(production_counts) if production_counts else 1
        sec = consumption_kwh / total_units if total_units > 0 else 0

        yield json.dumps({
            "device_id": device_id,
            "window_end": datetime.fromtimestamp(context.window().end / 1000.0).isoformat(),
            "consumption_kwh": round(consumption_kwh, 3),
            "co2_kg": round(co2_emissions, 3),
            "sec": round(sec, 4),
            "unit": "kWh"
        })

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # ── Source & Sink (Simplified for brevity) ──────────────────
    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(INPUT_TOPIC) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(STATS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

    # ── Pipeline ──────────────────────────────────────────────
    # Map raw strings to JSON and assign watermarks
    stream = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "Energy Source") \
                .map(lambda x: json.loads(x))

    # Windowing logic: Group by device, window by 1 hour
    stats = (
        stream
        .key_by(lambda msg: msg.get("device_id"))
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .process(EnergyStatsProcessFunction())
    )

    stats.sink_to(sink)
    env.execute("EMS Hourly Energy & CO2 Aggregator")

if __name__ == "__main__":
    main()
