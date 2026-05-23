#!/usr/bin/env python3
"""
Minnesota Weather Consumer
Reads alerts from Kafka, persists to Postgres, emails on severe/extreme alerts.
"""

import json
import os
import smtplib
import time
from datetime import datetime
from email.message import EmailMessage

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

load_dotenv()

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "mn-weather-alerts"

POSTGRES_DSN = (
    f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ.get('POSTGRES_DB', 'weather')} "
    f"user={os.environ.get('POSTGRES_USER', 'weather')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', 'weather')}"
)

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
NOTIFY_EMAIL = os.environ.get("NOTIFY_EMAIL", "")

ALERT_SEVERITIES = {"Extreme", "Severe"}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_alerts (
    id            TEXT PRIMARY KEY,
    event         TEXT,
    severity      TEXT,
    headline      TEXT,
    area          TEXT,
    onset         TIMESTAMPTZ,
    expires       TIMESTAMPTZ,
    description   TEXT,
    polled_at     TIMESTAMPTZ,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def make_consumer():
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="mn-weather-consumer",
            )
            print(f"✅ Connected to Kafka at {KAFKA_BROKER}")
            return consumer
        except NoBrokersAvailable:
            print(f"⏳ Kafka not ready, retrying in 5s (attempt {attempt + 1}/10)...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after 10 attempts")


def make_db():
    for attempt in range(10):
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
            print("✅ Connected to Postgres")
            return conn
        except psycopg2.OperationalError:
            print(f"⏳ Postgres not ready, retrying in 5s (attempt {attempt + 1}/10)...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Postgres after 10 attempts")


def insert_alert(conn, event):
    sql = """
        INSERT INTO weather_alerts
            (id, event, severity, headline, area, onset, expires, description, polled_at)
        VALUES
            (%(id)s, %(event)s, %(severity)s, %(headline)s, %(area)s,
             %(onset)s, %(expires)s, %(description)s, %(polled_at)s)
        ON CONFLICT (id) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, event)
        return cur.rowcount  # 1 = inserted, 0 = duplicate


def send_email(event):
    if not all([SMTP_USER, SMTP_PASSWORD, NOTIFY_EMAIL]):
        print("   ⚠️  SMTP not configured — skipping email")
        return

    msg = EmailMessage()
    msg["Subject"] = f"[MN Weather] {event['severity'].upper()}: {event['event']}"
    msg["From"] = SMTP_USER
    msg["To"] = NOTIFY_EMAIL
    msg.set_content(
        f"{event['headline']}\n\n"
        f"Area:    {event['area']}\n"
        f"Onset:   {event['onset']}\n"
        f"Expires: {event['expires']}\n\n"
        f"{event['description']}"
    )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_PASSWORD)
        smtp.send_message(msg)

    print(f"   📧 Email sent to {NOTIFY_EMAIL}")


def process(conn, event):
    inserted = insert_alert(conn, event)
    if inserted == 0:
        print(f"   ↩  Duplicate skipped: {event['id']}")
        return

    print(f"   ✅ Stored [{event['severity']}] {event['event']} ({event['area'][:50]})")

    if event.get("severity") in ALERT_SEVERITIES:
        try:
            send_email(event)
        except Exception as e:
            print(f"   ❌ Email failed: {e}")


if __name__ == "__main__":
    consumer = make_consumer()
    conn = make_db()

    print(f"👂 Listening on topic '{KAFKA_TOPIC}'...\n")
    for message in consumer:
        event = message.value
        print(f"\n📨 Received at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        process(conn, event)
