"""MQTT → Kafka bridge.

Replaces the Kafka producer action that EMQX 5 OSS lacks. Subscribes to
one or more MQTT topic filters and forwards each message onto a matching
Kafka topic, using the second topic segment (vehicle_id) as the
partition key so per-vehicle ordering is preserved.

Routing is driven by env var `ROUTES` (YAML list) for easy deployment;
a zero-config legacy mode using MQTT_TOPIC/KAFKA_TOPIC is preserved so
existing dev stacks keep working unchanged.

    ROUTES: |
      - mqtt: vehicle/+/frames
        kafka: raw.frame.metadata
      - mqtt: vehicle/+/gps
        kafka: raw.gps
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass

import paho.mqtt.client as mqtt
import yaml
from confluent_kafka import Producer
from prometheus_client import Counter, start_http_server


log = logging.getLogger("mqtt-kafka-bridge")

messages_bridged = Counter(
    "mqtt_kafka_bridge_messages_total",
    "Messages forwarded from MQTT to Kafka.",
    ["kafka_topic"],
)
buffer_full = Counter(
    "mqtt_kafka_bridge_buffer_full_total",
    "Times the Kafka producer buffer was full and forced a flush.",
)
unrouted = Counter(
    "mqtt_kafka_bridge_unrouted_total",
    "MQTT messages that didn't match any configured route (dropped).",
)


@dataclass(frozen=True)
class Route:
    mqtt_filter: str   # MQTT wildcard filter e.g. "vehicle/+/frames"
    kafka_topic: str   # Kafka topic to forward to


def extract_vehicle_id(topic: str) -> str:
    parts = topic.split("/")
    return parts[1] if len(parts) >= 2 else ""


def topic_matches(topic_filter: str, topic: str) -> bool:
    """Minimal MQTT topic wildcard matcher (+ and #)."""
    f = topic_filter.split("/")
    t = topic.split("/")
    for i, seg in enumerate(f):
        if seg == "#":
            return True
        if i >= len(t):
            return False
        if seg == "+":
            continue
        if seg != t[i]:
            return False
    return len(f) == len(t)


def load_routes() -> list[Route]:
    raw = os.getenv("ROUTES", "").strip()
    if raw:
        # YAML (or pragma-free JSON-like) list of {mqtt, kafka}.
        parsed = yaml.safe_load(raw)
        if not isinstance(parsed, list):
            raise ValueError("ROUTES must be a YAML list")
        routes = []
        for r in parsed:
            routes.append(Route(mqtt_filter=r["mqtt"], kafka_topic=r["kafka"]))
        return routes
    # Legacy single-route env vars.
    return [Route(
        mqtt_filter=os.getenv("MQTT_TOPIC", "vehicle/+/frames"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "raw.frame.metadata"),
    )]


def build_on_message(producer: Producer, routes: list[Route]):
    def on_message(_client, _userdata, msg: mqtt.MQTTMessage) -> None:
        route = next((r for r in routes if topic_matches(r.mqtt_filter, msg.topic)), None)
        if route is None:
            unrouted.inc()
            return
        key = extract_vehicle_id(msg.topic).encode("utf-8")
        try:
            producer.produce(route.kafka_topic, key=key, value=msg.payload)
            producer.poll(0)
            messages_bridged.labels(kafka_topic=route.kafka_topic).inc()
        except BufferError:
            buffer_full.inc()
            log.warning("producer buffer full; flushing")
            producer.flush()
            producer.produce(route.kafka_topic, key=key, value=msg.payload)
            messages_bridged.labels(kafka_topic=route.kafka_topic).inc()

    return on_message


def build_on_connect(routes: list[Route]):
    def on_connect(client: mqtt.Client, _userdata, _flags, reason_code, _props=None) -> None:
        if reason_code == 0:
            for r in routes:
                log.info("mqtt subscribing: %s -> %s", r.mqtt_filter, r.kafka_topic)
                client.subscribe(r.mqtt_filter, qos=1)
        else:
            log.error("mqtt connect failed: %s", reason_code)

    return on_connect


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    mqtt_host = os.getenv("MQTT_HOST", "emqx")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

    routes = load_routes()
    if not routes:
        log.error("no routes configured")
        return 2

    metrics_port = int(os.getenv("METRICS_PORT", "2117"))
    if metrics_port > 0:
        start_http_server(metrics_port)
        log.info("metrics on :%d", metrics_port)

    log.info("bridge starting: mqtt=%s:%d -> kafka=%s  routes=%s",
             mqtt_host, mqtt_port, kafka_bootstrap,
             [(r.mqtt_filter, r.kafka_topic) for r in routes])

    producer = Producer({
        "bootstrap.servers": kafka_bootstrap,
        "linger.ms": 20,
        "compression.type": "lz4",
        "enable.idempotence": True,
    })

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt-kafka-bridge")
    client.on_connect = build_on_connect(routes)
    client.on_message = build_on_message(producer, routes)

    stop_event = threading.Event()

    def _stop(signum, _frame):
        log.info("signal %d received; stopping", signum)
        stop_event.set()
        client.disconnect()
        producer.flush(timeout=5)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    client.connect(mqtt_host, mqtt_port, keepalive=30)
    client.loop_forever(retry_first_connection=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
