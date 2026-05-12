import json
import os
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer
)
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema


# ── Constants ────────────────────────────────────────────────────────────────
KAFKA_BROKER        = os.getenv("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC         = "ems.meters.1"      # Actual topic 
OUTPUT_TOPIC        = "ems.alerts"

VOLTAGE_NOMINAL     = 230.0               # V
FREQUENCY_NOMINAL   = 50.0               # Hz
DEVIATION_PCT       = 0.05               # ±5%
SUSTAINED_MS        = 3000               # 3 000 ms = 3 seconds

# Derived limits — computed once at module level
V_LOW   = VOLTAGE_NOMINAL   * (1 - DEVIATION_PCT)   # 218.5 V
V_HIGH  = VOLTAGE_NOMINAL   * (1 + DEVIATION_PCT)   # 241.5 V
F_LOW   = FREQUENCY_NOMINAL * (1 - DEVIATION_PCT)   # 47.5 Hz
F_HIGH  = FREQUENCY_NOMINAL * (1 + DEVIATION_PCT)   # 52.5 Hz


def build_alarm(device_id, device_name, alarm_type, priority, value, threshold, unit, message):
    """Return a serialised AlarmEvent JSON string ready for the ems.alerts topic."""
    return json.dumps({
        "event_type":  "AlarmEvent",
        "device_id":   device_id,
        "device_name": device_name,
        "alarm_type":  alarm_type,    # OVERVOLTAGE | UNDERVOLTAGE | FREQ_HIGH | FREQ_LOW
        "priority":    priority,      # HIGH | MEDIUM | LOW
        "value":       value,
        "threshold":   threshold,
        "unit":        unit,
        "message":     message,
        "timestamp":   datetime.utcnow().isoformat() + "Z"
    })


class ThresholdAlertFunction(KeyedProcessFunction):
    """
    Keyed per device_id (e.g. "PM-001", "PM-002").

    Reads the nested measurements block from your Node-RED schema:
    {
        "device_id":   "PM-001",
        "device_name": "Power Meter 1",
        "timestamp":   "...",
        "measurements": {
            "frequency_Hz":      50.1,
            "voltage_V":         231.4,
            "current_A":         12.0,
            "power_factor":      0.95,
            "thd_voltage_pct":   2.1,
            "thd_current_pct":   4.3,
            "active_energy_kWh": 1024
        }
    }

    Rules implemented
    ─────────────────
    1. voltage_V   outside ±5% of 230 V for > 3 s  → OVERVOLTAGE / UNDERVOLTAGE
    2. frequency_Hz outside ±5% of 50 Hz for > 3 s → FREQ_HIGH / FREQ_LOW
    """

    def open(self, runtime_context):
        # Timestamp (epoch ms) when voltage FIRST went out of range.
        # None means it is currently within nominal range.
        self.voltage_fault_start = runtime_context.get_state(
            ValueStateDescriptor("voltage_fault_start", Types.LONG())
        )
        # Same for frequency
        self.freq_fault_start = runtime_context.get_state(
            ValueStateDescriptor("freq_fault_start", Types.LONG())
        )

    def process_element(self, raw_value, ctx: KeyedProcessFunction.Context):
        try:
            msg          = json.loads(raw_value)
            device_id    = msg.get("device_id",   "unknown")
            device_name  = msg.get("device_name", device_id)

            # ── Pull the nested measurements block ────────────────────────
            m = msg.get("measurements", {})
            if not m:
                # Message arrived without measurements — skip silently
                return

            voltage   = m.get("voltage_V")
            frequency = m.get("frequency_Hz")

            # Wall-clock fallback when event-time watermarks are not configured
            now_ms = ctx.timestamp() or int(datetime.utcnow().timestamp() * 1000)

            # ── 1. Voltage check ──────────────────────────────────────────
            if voltage is not None:
                v_out = voltage < V_LOW or voltage > V_HIGH

                if v_out:
                    if self.voltage_fault_start.value() is None:
                        # First sample outside range — start the fault clock
                        self.voltage_fault_start.update(now_ms)
                    else:
                        duration_ms = now_ms - self.voltage_fault_start.value()
                        if duration_ms >= SUSTAINED_MS:
                            alarm_type = "OVERVOLTAGE" if voltage > V_HIGH else "UNDERVOLTAGE"
                            yield build_alarm(
                                device_id   = device_id,
                                device_name = device_name,
                                alarm_type  = alarm_type,
                                priority    = "MEDIUM",
                                value       = round(voltage, 2),
                                threshold   = f"{V_LOW}–{V_HIGH} V",
                                unit        = "V",
                                message     = (
                                    f"{device_name} voltage {voltage:.1f} V is outside "
                                    f"±5% of {VOLTAGE_NOMINAL} V nominal "
                                    f"for {duration_ms} ms."
                                )
                            )
                            # Clear so we don't flood — next alarm only after recovery + new fault
                            self.voltage_fault_start.clear()
                else:
                    # Recovered — reset fault clock
                    self.voltage_fault_start.clear()

            # ── 2. Frequency check ────────────────────────────────────────
            if frequency is not None:
                f_out = frequency < F_LOW or frequency > F_HIGH

                if f_out:
                    if self.freq_fault_start.value() is None:
                        self.freq_fault_start.update(now_ms)
                    else:
                        duration_ms = now_ms - self.freq_fault_start.value()
                        if duration_ms >= SUSTAINED_MS:
                            alarm_type = "FREQ_HIGH" if frequency > F_HIGH else "FREQ_LOW"
                            yield build_alarm(
                                device_id   = device_id,
                                device_name = device_name,
                                alarm_type  = alarm_type,
                                priority    = "MEDIUM",
                                value       = round(frequency, 3),
                                threshold   = f"{F_LOW}–{F_HIGH} Hz",
                                unit        = "Hz",
                                message     = (
                                    f"{device_name} frequency {frequency:.2f} Hz is outside "
                                    f"±5% of {FREQUENCY_NOMINAL} Hz nominal "
                                    f"for {duration_ms} ms."
                                )
                            )
                            self.freq_fault_start.clear()
                else:
                    self.freq_fault_start.clear()

        except json.JSONDecodeError as e:
            print(f"[WARN] Bad JSON from Kafka: {e} | raw: {raw_value[:120]}")
        except Exception as e:
            print(f"[ERROR] Unexpected error processing message: {e} | raw: {raw_value[:120]}")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)   # 1 is fine on dev VM; match to Kafka partition count in prod

    # ── Source: ems.sensors ───────────────────────────────────────────────────
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(INPUT_TOPIC)
        .set_group_id("flink-threshold-monitor")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # ── Sink: ems.alerts ──────────────────────────────────────────────────────
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # ── Pipeline ──────────────────────────────────────────────────────────────
    stream = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka: ems.sensors")
    )

    alerts = (
        stream
        # Key by device_id so each PM gets its own independent fault state
        # After — skips empty/whitespace strings before parsing
        .filter(lambda raw: raw is not None and raw.strip() != "")
        .map(lambda x: (print(f"[FLINK DEBUG] {x}"), x)[1]) 
        .key_by(lambda raw: json.loads(raw).get("device_id", "unknown"))
        .process(ThresholdAlertFunction())
    )

    alerts.sink_to(sink)
    env.execute("EMS Threshold Alert Monitor — v1")


if __name__ == "__main__":
    main()
