#!/bin/bash
# scripts/kafka-create-topics.sh
# KRaft mode — targets the INTERNAL listener kafka:9092 inside iot-net.
# Safe to re-run: --if-not-exists skips already-created topics.

set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka:9092}"
LINES=("L1")
AREAS=("extraction" "washing" "flotation" "utilities" "storage_handling")
TYPES=("pm" "pv")

echo "⏳ Waiting for Kafka KRaft broker at $BROKER..."
until kafka-topics --bootstrap-server "$BROKER" --list &>/dev/null; do
  echo "   not ready yet, retrying in 3s..."
  sleep 3
done
echo "✅ Kafka is ready"
echo ""

create_topic() {
  local TOPIC="$1"
  local PARTITIONS="$2"
  kafka-topics --bootstrap-server "$BROKER" \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor 1
  echo "  ✓ $TOPIC  (partitions: $PARTITIONS)"
}

echo "📦 Creating area × payload-type topics..."
for LINE in "${LINES[@]}"; do
  for AREA in "${AREAS[@]}"; do
    for TYPE in "${TYPES[@]}"; do
      # FIX 4: 4 partitions — matches KAFKA_NUM_PARTITIONS and Flink parallelism
      create_topic "ems.${LINE}.${AREA}.${TYPE}" 4
    done
  done

  echo ""
  echo "📦 Creating special topics for line $LINE..."
  create_topic "ems.${LINE}.utilities.steam_fuel"  2
  create_topic "ems.${LINE}.energy_consumption"    2
  create_topic "ems.${LINE}.water_consumption"     2
done

echo ""
echo "📦 Creating shared dead-letter topic..."
create_topic "ems.dlq" 2

echo ""
echo "Creating normalized KPI input topics..."
create_topic "ems.normalized.electrical_measurements" 4
create_topic "ems.normalized.process_variables" 4
create_topic "ems.normalized.steam_fuel_measurements" 2
create_topic "ems.normalized.water_consumption" 2

echo ""
echo "✅ All done. Current EMS topics:"
kafka-topics --bootstrap-server "$BROKER" --list | grep "^ems\." | sort
