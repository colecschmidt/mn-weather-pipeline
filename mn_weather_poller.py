#!/usr/bin/env python3
"""
Minnesota Weather Poller
Pulls active NOAA alerts for Minnesota and publishes them to Kafka.
This is the "pump jack" — step 1 of the weather data pipeline.
"""

import requests
import json
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

NOAA_ALERTS_URL = "https://api.weather.gov/alerts/active"
STATE = "MN"
HEADERS = {
    "User-Agent": "mn-weather-pipeline/0.1 (your@email.com)",
    "Accept": "application/geo+json"
}

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "mn-weather-alerts"
POLL_INTERVAL = 60


def make_producer():
    """Connect to Kafka with retries — broker may not be ready yet."""
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print(f"✅ Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Kafka not ready, retrying in 5s (attempt {attempt + 1}/10)...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after 10 attempts")


def fetch_alerts():
    response = requests.get(
        NOAA_ALERTS_URL,
        headers=HEADERS,
        params={"area": STATE},
        timeout=10
    )
    response.raise_for_status()
    return response.json()


def build_event(alert):
    """Extract the fields we care about into a clean event."""
    props = alert.get("properties", {})
    return {
        "id":          props.get("id", ""),
        "event":       props.get("event", ""),
        "severity":    props.get("severity", ""),
        "headline":    props.get("headline", ""),
        "area":        props.get("areaDesc", ""),
        "onset":       props.get("onset", ""),
        "expires":     props.get("expires", ""),
        "description": props.get("description", "").strip(),
        "polled_at":   datetime.utcnow().isoformat()
    }


def poll_and_publish(producer):
    print(f"\n🌨  Polling NOAA at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        data = fetch_alerts()
    except requests.RequestException as e:
        print(f"❌ NOAA request failed: {e}")
        return

    features = data.get("features", [])
    if not features:
        print("✅ No active alerts in Minnesota.")
        return

    print(f"⚠️  {len(features)} alert(s) — publishing to Kafka topic '{KAFKA_TOPIC}'")
    for alert in features:
        event = build_event(alert)
        producer.send(KAFKA_TOPIC, value=event)
        print(f"   → [{event['severity']}] {event['event']} ({event['area'][:50]})")

    producer.flush()
    print(f"✅ Published {len(features)} event(s)")


if __name__ == "__main__":
    producer = make_producer()
    while True:
        poll_and_publish(producer)
        print(f"⏳ Sleeping {POLL_INTERVAL}s...\n")
        time.sleep(POLL_INTERVAL)