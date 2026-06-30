"use strict";

const mqtt  = require("mqtt");
const { Kafka } = require("kafkajs");

// ── Config ────────────────────────────────────────────────────────────────────
const MQTT_BROKER  = process.env.MQTT_BROKER  || "mqtt://mqtt-broker:1883";
const KAFKA_BROKER = process.env.KAFKA_BROKER || "kafka:9092";

console.log("MQTT_BROKER  =", MQTT_BROKER);
console.log("KAFKA_BROKER =", KAFKA_BROKER);

// ── Topic resolution ──────────────────────────────────────────────────────────
//
// MQTT path structure:
//   Al_Youssoufia_Plant / Line-1 / <area> / <equipment> / <measurementType>
//   [0]                   [1]      [2]       [3]           [4]
//
// Kafka topic convention:
//   ems.<line>.<area>.<payloadType>
//
//   <line>        : derived from segments[1]  e.g. "Line-1" → "L1"
//   <area>        : derived from segments[2]  e.g. "Extraction" → "extraction"
//   <payloadType> : derived from segments[4]  e.g. "PM1" → "pm"
//                                              "Process_Variables" → "pv"
//                                              special cases below
//
// Special cases:
//   Total_water_consumption  → no segments[3]/[4], sits at [2]
//   Energy_consumption/<AREA>→ [2] = Energy_consumption, [3] = area name
//   Steam_Fuel_Meter         → [3] = Steam_Fuel_Meter, no device_id
//

// Area segments that map to a known stream
const AREA_MAP = {
  Extraction:         "extraction",
  Washing:            "washing",
  Flotation:          "flotation",
  Utilities:          "utilities",
  Storage_Handling:   "storage_handling",
};

// Measurement-type suffix detection (segment[4])
// Anything starting with "PM" (PM1, PM2 …) → "pm"
// "Process_Variables"                        → "pv"
function payloadType(measurementSegment) {
  if (!measurementSegment) return null;
  if (/^PM\d*/i.test(measurementSegment)) return "pm";
  if (measurementSegment === "Process_Variables") return "pv";
  return null;
}

// Line segment normalisation: "Line-1" → "L1", "Line-2" → "L2", etc.
function normaliseLine(lineSegment) {
  if (!lineSegment) return "Lx";
  const m = lineSegment.match(/Line-(\d+)/i);
  return m ? `L${m[1]}` : lineSegment.toLowerCase().replace(/[^a-z0-9]/g, "");
}

/**
 * Resolve MQTT topic → Kafka topic.
 *
 * Returns:
 *   { kafkaTopic, line, area, type, isDlq }
 */
function resolveKafkaTopic(mqttTopic) {
  const seg  = mqttTopic.split("/");
  // seg[0] = plant name (ignored — single plant for now)
  const line = normaliseLine(seg[1]);   // e.g. "L1"
  const area = seg[2] || "";            // e.g. "Extraction"
  const meas = seg[4] || "";            // e.g. "PM1" | "Process_Variables"

  // ── Aggregate topics ──────────────────────────────────────────────────────
  if (area === "Total_water_consumption") {
    return { kafkaTopic: `ems.${line}.water_consumption`, line, area, type: "agg", isDlq: false };
  }
  if (area === "Energy_consumption") {
    return { kafkaTopic: `ems.${line}.energy_consumption`, line, area, type: "agg", isDlq: false };
  }

  // ── Steam / Fuel meter (Utilities, no device_id, unique structure) ─────────
  if (area === "Utilities" && seg[3] === "Steam_Fuel_Meter") {
    return { kafkaTopic: `ems.${line}.utilities.steam_fuel`, line, area, type: "steam_fuel", isDlq: false };
  }

  // ── Standard area + payload-type routing ──────────────────────────────────
  const areaNorm = AREA_MAP[area];
  const type     = payloadType(meas);

  if (areaNorm && type) {
    return { kafkaTopic: `ems.${line}.${areaNorm}.${type}`, line, area, type, isDlq: false };
  }

  // ── Dead-letter ───────────────────────────────────────────────────────────
  return { kafkaTopic: "ems.dlq", line, area, type: "unknown", isDlq: true };
}

// ── Kafka ─────────────────────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: "mqtt-kafka-bridge",
  brokers: [KAFKA_BROKER],
  retry: { retries: 10, initialRetryTime: 300, factor: 2 },
});

const producer = kafka.producer({
  allowAutoTopicCreation: false,
  idempotent: true,
});

async function connectKafkaWithRetry() {
  while (true) {
    try {
      await producer.connect();
      console.log("✅ Connected to Kafka");
      break;
    } catch (err) {
      console.warn("⏳ Kafka not ready, retrying in 5s…", err.message);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

// ── MQTT ──────────────────────────────────────────────────────────────────────
const mqttClient = mqtt.connect(MQTT_BROKER, {
  reconnectPeriod: 2000,
  connectTimeout:  10000,
  clientId: `mqtt-kafka-bridge-${process.pid}`,
});

// ── Stats ─────────────────────────────────────────────────────────────────────
const stats = { received: 0, forwarded: 0, dlq: 0, errors: 0 };
setInterval(() => {
  console.log(
    `📊 received:${stats.received}  forwarded:${stats.forwarded}  dlq:${stats.dlq}  errors:${stats.errors}`
  );
}, 60_000);

// ── Main ──────────────────────────────────────────────────────────────────────
async function run() {

  mqttClient.on("connect", () => {
    console.log("✅ Connected to MQTT broker");
    mqttClient.subscribe("#", { qos: 1 }, (err, granted) => {
      if (err) console.error("❌ Subscribe failed:", err);
      else      console.log("📡 Subscribed to '#':", granted);
    });
  });

  mqttClient.on("message", async (mqttTopic, payload) => {
    stats.received++;
    const { kafkaTopic, line, area, type, isDlq } = resolveKafkaTopic(mqttTopic);

    if (isDlq) {
      console.warn(`⚠️  No route: "${mqttTopic}" → ems.dlq  (area="${area}" type="${type}")`);
      stats.dlq++;
    } else {
      console.log(`📩 [${line}/${area}/${type}] ${payload.length}B → ${kafkaTopic}`);
    }

    try {
      await producer.send({
        topic: kafkaTopic,
        messages: [{
          // Key = device_id extracted from payload when available;
          // falls back to full MQTT topic path.
          // Using device_id as key guarantees all messages for one device
          // land on the same Kafka partition → ordered stream per device in Flink.
          key:   extractDeviceKey(payload, mqttTopic),
          value: payload,
          timestamp: Date.now().toString(),
          headers: {
            "mqtt-topic":  mqttTopic,
            "source-line": line,
            "source-area": area,
            "payload-type": type,
          },
        }],
      });
      stats.forwarded++;
    } catch (err) {
      stats.errors++;
      console.error(`❌ Kafka send error [${kafkaTopic}]:`, err.message);
    }
  });

  mqttClient.on("error",     (err) => console.error("MQTT error:", err));
  mqttClient.on("reconnect", ()    => console.log("♻️  MQTT reconnecting…"));
  mqttClient.on("close",     ()    => console.log("🔌 MQTT connection closed"));
  mqttClient.on("offline",   ()    => console.warn("📴 MQTT client offline"));

  await connectKafkaWithRetry();
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Fast device_id extraction without full JSON.parse.
 * Regex is safe here — we only need one top-level string field.
 * Falls back to the MQTT topic string if not found.
 *
 * Why not JSON.parse? At high throughput (100+ msg/s) avoiding a full
 * parse in the bridge keeps CPU budget for Flink where the real work happens.
 */
function extractDeviceKey(payloadBuffer, fallback) {
  try {
    const raw = payloadBuffer.toString("utf8", 0, 256); // peek first 256 bytes only
    const m   = raw.match(/"device_id"\s*:\s*"([^"]+)"/);
    if (m) return Buffer.from(m[1]);
  } catch (_) { /* ignore */ }
  return Buffer.from(fallback);
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────
async function shutdown(signal) {
  console.log(`\n${signal} — shutting down…`);
  try {
    mqttClient.end(true);
    await producer.disconnect();
    console.log("👋 Clean shutdown");
  } catch (err) {
    console.error("Shutdown error:", err);
  }
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT",  () => shutdown("SIGINT"));

run();
