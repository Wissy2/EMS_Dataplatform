import json
import os
from datetime import datetime
from pyflink.common import WatermarkStrategy, Time, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction

# ── Constants ────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
INPUT_TOPIC  = "ems.meters.1"
PEAK_TOPIC   = "ems.energy.peaks"

# Peak Demand Threshold (Example: 500kW)
PEAK_LIMIT_KW = 500.0 

class PeakDemandProcessFunction(ProcessWindowFunction):
    """
    Calculates the 15-minute moving average of Power (kW).
    Reports if demand is approaching the contract limit.
    """
    def process(self, key, context, elements):
        device_id = key
        # Extract Real Power (P) values
        power_values = [msg.get("real_power_total") for msg in elements if "real_power_total" in msg]

        if not power_values:
            return

        avg_demand = sum(power_values) / len(power_values)
        peak_status = "CRITICAL" if avg_demand >= PEAK_LIMIT_KW else "NORMAL"
        
        # Calculate 'Approaching Peak' warning (at 90% of limit)
        if peak_status == "NORMAL" and avg_demand >= (PEAK_LIMIT_KW * 0.9):
            peak_status = "WARNING"

        yield json.dumps({
            "device_id": device_id,
            "window_end": datetime.fromtimestamp(context.window().end / 1000.0).isoformat(),
            "avg_demand_kw": round(avg_demand, 2),
            "peak_limit_kw": PEAK_LIMIT_KW,
            "status": peak_status,
            "sample_count": len(power_values)
        })

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # Kafka Source Configuration
    # ... (Same as previous scripts)

    # ── Pipeline ──────────────────────────────────────────────
    stream = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "Sensor Stream") \
                .map(lambda x: json.loads(x))

    # Sliding Window: 15 minutes long, updates every 1 minute
    peak_analysis = (
        stream
        .key_by(lambda msg: msg.get("device_id"))
        .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(1)))
        .process(PeakDemandProcessFunction())
    )

    peak_analysis.sink_to(sink)
    env.execute("EMS Peak Demand Monitor")
