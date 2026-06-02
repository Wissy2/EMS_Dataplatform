import json
import os
from datetime import datetime
from pyflink.common import WatermarkStrategy, Types, Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction

# ── Constants ────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
INPUT_TOPIC  = "ems.meters.1"
STATS_TOPIC  = "ems.energy.stats"

# CO2 Factors
CO2_FACTOR_ELEC = 0.63   # kg CO2 per kWh  (Morocco avg)
CO2_FACTOR_FUEL = 2.68   # kg CO2 per litre of fuel


class EnergyStatsProcessFunction(ProcessWindowFunction):
    """
    Processes a 1-hour window to calculate:
    - Total kWh consumed in that hour
    - CO2 emissions from electricity
    - CO2 emissions from fuel:  CO2_fuel = F_jour (L) * 2.68 kg/L
    - Total combined CO2 emissions
    - Specific Energy Consumption (if production data is available)
    """

    def process(self, key, context, elements):
        device_id = key

        # Collect relevant fields from every message in the window
        energy_values      = []
        production_counts  = []
        fuel_volumes       = []   # F_jour in litres, field name: "fuel_consumption_L"

        for msg in elements:
            if "active_energy_kWh" in msg:
                energy_values.append(msg["active_energy_kWh"])
            if "units_produced" in msg:
                production_counts.append(msg["units_produced"])
            if "fuel_consumption_L" in msg:
                fuel_volumes.append(msg["fuel_consumption_L"])

        if not energy_values:
            return

        # ── Electricity ──────────────────────────────────────
        # Consumption = Max − Min register reading inside the window
        consumption_kwh   = max(energy_values) - min(energy_values)
        co2_electricity   = consumption_kwh * CO2_FACTOR_ELEC

        # ── Fuel ─────────────────────────────────────────────
        # F_jour = total fuel consumed during the window (litres)
        # CO2_fuel = F_jour (L) * 2.68 kg/L
        f_jour            = sum(fuel_volumes) if fuel_volumes else 0.0
        co2_fuel          = f_jour * CO2_FACTOR_FUEL

        # ── Combined CO2 ─────────────────────────────────────
        co2_total         = co2_electricity + co2_fuel

        # ── Specific Energy Consumption (SEC) ────────────────
        total_units = sum(production_counts) if production_counts else 0
        sec         = consumption_kwh / total_units if total_units > 0 else 0.0

        yield json.dumps({
            "device_id":          device_id,
            "window_end":         datetime.fromtimestamp(
                                      context.window().end / 1000.0
                                  ).isoformat(),
            # Electricity
            "consumption_kwh":    round(consumption_kwh, 3),
            "co2_electricity_kg": round(co2_electricity, 3),
            # Fuel
            "fuel_consumption_L": round(f_jour, 3),
            "co2_fuel_kg":        round(co2_fuel, 3),
            # Totals
            "co2_total_kg":       round(co2_total, 3),
            "sec":                round(sec, 4),
            "unit":               "kWh"
        })


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # ── Source ────────────────────────────────────────────────
    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(INPUT_TOPIC) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # ── Sink ──────────────────────────────────────────────────
    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(STATS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

    # ── Pipeline ──────────────────────────────────────────────
    stream = (
        env.from_source(
            source,
            WatermarkStrategy.for_monotonous_timestamps(),
            "Energy Source"
        )
        .map(lambda x: json.loads(x))
    )

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
