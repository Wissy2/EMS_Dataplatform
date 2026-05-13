const mqtt = require("mqtt");
const { Kafka } = require("kafkajs");

const MQTT_BROKER = process.env.MQTT_BROKER || "mqtt://mqtt-broker:1883";
const KAFKA_BROKER = process.env.KAFKA_BROKER || "kafka:29092";

console.log("MQTT_BROKER =", MQTT_BROKER);
console.log("KAFKA_BROKER =", KAFKA_BROKER);

const MQTT_TOPIC = "ems/meters/1";
const KAFKA_TOPIC = "ems.meters.1";

const mqttClient = mqtt.connect(MQTT_BROKER, {
  reconnectPeriod: 2000,
});

const kafka = new Kafka({
  clientId: "mqtt-kafka-bridge",
  brokers: [KAFKA_BROKER],
});

const producer = kafka.producer();

async function connectKafkaWithRetry() {
  while (true) {
    try {
      await producer.connect();
      console.log("✅ Connected to Kafka");
      break;
    } catch (err) {
      console.log("⏳ Kafka not ready, retrying in 5s...");
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

async function run() {
  mqttClient.on("connect", () => {
    console.log("✅ Connected to MQTT broker");
    mqttClient.subscribe("#", (err, granted) => {
      if (err) {
        console.error("❌ Subscribe failed:", err);
      } else {
        console.log("📡 Subscribed to MQTT:", granted);
      }
    });
  });

  mqttClient.on("message", async (topic, payload) => {
    // ← CHANGED: log buffer length instead of toString() to avoid corrupting binary
    console.log(`📩 MQTT RAW: ${topic} [${payload.length} bytes]`);

    try {
      await producer.send({
        topic: KAFKA_TOPIC,
        messages: [
          {
            key: topic,
            value: payload,           // ← CHANGED: pass Buffer directly, no toString()
            timestamp: Date.now().toString(),  // ← CHANGED: fixed the [Date.now] typo
          },
        ],
      });
      console.log(`🚀 Forwarded [${topic}] → Kafka [${payload.length} bytes]`);
    } catch (err) {
      console.error("❌ Kafka send error:", err);
    }
  });

  mqttClient.on("error", (err) => console.error("MQTT error:", err));
  mqttClient.on("reconnect", () => console.log("♻️ MQTT reconnecting..."));
  mqttClient.on("close", () => console.log("🔌 MQTT connection closed"));

  await connectKafkaWithRetry();
}

run();
