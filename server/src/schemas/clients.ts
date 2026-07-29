import { Kafka } from "kafkajs";
import { createClient } from "redis";

export const redis = createClient({ url: 'redis://127.0.0.1:6379' });
redis.on("error", (err) => console.error("Redis Client Error", err));
await redis.connect();

const kafka = new Kafka({
  clientId: "cex-y-server",
  brokers: ["localhost:9092"]
});

export const producer = kafka.producer();