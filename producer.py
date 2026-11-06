"""Kafka producer for the streaming dashboard, backed by WeatherAPI.com.

Replaces the template's synthetic data generator with real readings from
https://www.weatherapi.com/ (current.json endpoint). Every API response is split
into one Kafka message per metric.

Message schema (JSON, UTF-8):
    timestamp    ISO 8601 string, UTC, time the reading was published   (required)
    value        number                                                 (required)
    metric_type  string, e.g. "temperature_c"                           (required)
    sensor_id    string, city slug, e.g. "quezon_city"                  (required)
    location     string, "Quezon City, Philippines"                     (extra)
    unit         string, e.g. "°C"                                      (extra)
    observed_at  ISO 8601 string, when WeatherAPI measured it           (extra)

Usage:
    python producer.py
    python producer.py --bootstrap-servers localhost:9092 --topic streaming-data --interval 15
    python producer.py --duration 300
"""
import argparse
import json
import logging
import os
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

load_dotenv()

API_URL = "https://api.weatherapi.com/v1/current.json"
REQUEST_TIMEOUT = 10  # seconds
MAX_RETRIES = 3

# metric_type -> (field inside the API's "current" object, display unit)
METRICS: Dict[str, tuple] = {
    "temperature_c": ("temp_c", "°C"),
    "feelslike_c": ("feelslike_c", "°C"),
    "humidity_pct": ("humidity", "%"),
    "wind_kph": ("wind_kph", "kph"),
    "pressure_mb": ("pressure_mb", "mb"),
    "uv_index": ("uv", ""),
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("producer")


class FatalAPIError(Exception):
    """An API error that retrying cannot fix (for example an invalid API key)."""


def slugify(city: str) -> str:
    """'Quezon City' -> 'quezon_city'."""
    return re.sub(r"[^a-z0-9]+", "_", city.lower()).strip("_")


class StreamingDataProducer:
    """Fetches weather readings and publishes them to a Kafka topic."""

    def __init__(self, bootstrap_servers: str, topic: str, api_key: str, cities: List[str]):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.api_key = api_key
        self.cities = cities
        self.stop_event = threading.Event()
        self.session = requests.Session()
        self.producer: Optional[KafkaProducer] = None
        self.sent = 0
        self.failed = 0

    # ------------------------------------------------------------------ Kafka
    def connect(self) -> bool:
        """Connect to Kafka, retrying every 5 s until reachable or stopped."""
        while not self.stop_event.is_set():
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    acks="all",
                    retries=5,
                    linger_ms=20,
                )
                logger.info("Connected to Kafka at %s", self.bootstrap_servers)
                return True
            except NoBrokersAvailable:
                logger.error("Kafka broker not reachable at %s. Retrying in 5 s...", self.bootstrap_servers)
                self.stop_event.wait(5)
        return False

    def serialize_data(self, record: dict) -> bytes:
        """Encode a record as UTF-8 JSON."""
        return json.dumps(record, ensure_ascii=False).encode("utf-8")

    def send_message(self, record: dict) -> bool:
        """Send one record and wait up to 10 s for the broker's acknowledgement."""
        try:
            future = self.producer.send(
                self.topic,
                key=record["sensor_id"].encode("utf-8"),  # same city -> same partition -> ordered
                value=self.serialize_data(record),
            )
            future.get(timeout=10)
            self.sent += 1
            return True
        except KafkaError as exc:
            self.failed += 1
            logger.error("Kafka delivery failed for %s/%s: %s", record["sensor_id"], record["metric_type"], exc)
            return False

    # -------------------------------------------------------------- Weather API
    def fetch_weather_data(self, city: str) -> Optional[dict]:
        """Return the parsed API JSON for one city, or None if it could not be fetched."""
        for attempt in range(1, MAX_RETRIES + 1):
            if self.stop_event.is_set():
                return None
            try:
                resp = self.session.get(
                    API_URL,
                    params={"key": self.api_key, "q": city, "aqi": "no"},
                    timeout=REQUEST_TIMEOUT,
                )
                if resp.status_code == 401:
                    raise FatalAPIError("WeatherAPI rejected the API key (HTTP 401). Check WEATHERAPI_KEY.")
                if resp.status_code == 400:
                    logger.error("WeatherAPI could not resolve location '%s' (HTTP 400). Skipping.", city)
                    return None
                if resp.status_code == 403:
                    logger.error("WeatherAPI returned 403 for '%s' (quota exceeded or key disabled). Skipping.", city)
                    return None
                resp.raise_for_status()
                return resp.json()
            except FatalAPIError:
                raise
            except (requests.RequestException, ValueError) as exc:
                logger.warning("Fetch failed for %s (attempt %d/%d): %s", city, attempt, MAX_RETRIES, exc)
                if attempt < MAX_RETRIES:
                    self.stop_event.wait(2 ** attempt)  # exponential backoff
        return None

    def build_records(self, city: str, payload: dict) -> List[dict]:
        """Turn one API response into one record per metric, dropping invalid values."""
        current = (payload or {}).get("current")
        if not isinstance(current, dict):
            logger.warning("Response for %s has no 'current' block; skipping.", city)
            return []
        loc = payload.get("location") or {}
        location = ", ".join(p for p in (loc.get("name"), loc.get("country")) if p) or city
        observed_at = None
        epoch = current.get("last_updated_epoch")
        if isinstance(epoch, (int, float)) and not isinstance(epoch, bool):
            observed_at = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

        timestamp = datetime.now(timezone.utc).isoformat()
        records = []
        for metric, (field, unit) in METRICS.items():
            value = current.get(field)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                logger.debug("Skipping %s/%s: non-numeric value %r", city, metric, value)
                continue
            records.append(
                {
                    "timestamp": timestamp,
                    "value": float(value),
                    "metric_type": metric,
                    "sensor_id": slugify(city),
                    "location": location,
                    "unit": unit,
                    "observed_at": observed_at,
                }
            )
        return records

    # ------------------------------------------------------------------- Loop
    def produce_stream(self, interval: float = 15.0, duration: Optional[float] = None) -> int:
        """Poll the API for every city each `interval` seconds until stopped or `duration` elapses."""
        if not self.connect():
            return 0
        logger.info(
            "Producing to topic '%s' for %s every %.0f s%s",
            self.topic,
            ", ".join(self.cities),
            interval,
            f" for {duration:.0f} s" if duration else " (Ctrl+C to stop)",
        )
        started = time.monotonic()
        try:
            while not self.stop_event.is_set():
                cycle_start = time.monotonic()
                for city in self.cities:
                    if self.stop_event.is_set():
                        break
                    payload = self.fetch_weather_data(city)
                    if payload is None:
                        continue
                    for record in self.build_records(city, payload):
                        self.send_message(record)
                logger.info("Cycle complete. Sent: %d, failed: %d", self.sent, self.failed)
                if duration and time.monotonic() - started >= duration:
                    break
                elapsed = time.monotonic() - cycle_start
                self.stop_event.wait(max(0.0, interval - elapsed))
        except FatalAPIError as exc:
            logger.error("%s", exc)
            return 1
        finally:
            self.close()
        return 0

    def close(self) -> None:
        """Flush pending messages and release connections."""
        logger.info("Flushing pending messages...")
        if self.producer is not None:
            try:
                self.producer.flush(timeout=10)
            except KafkaError as exc:
                logger.error("Flush failed: %s", exc)
            finally:
                self.producer.close()
                self.producer = None
        self.session.close()
        logger.info("Producer stopped. Total sent: %d, failed: %d", self.sent, self.failed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WeatherAPI.com -> Kafka producer")
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic", default=os.getenv("KAFKA_TOPIC", "streaming-data"),
        help="Kafka topic (default: streaming-data)",
    )
    parser.add_argument(
        "--interval", type=float, default=float(os.getenv("POLL_INTERVAL_SECONDS", "15")),
        help="Seconds between API polling cycles (default: 15)",
    )
    parser.add_argument(
        "--duration", type=float, default=None,
        help="Stop after this many seconds (default: run until Ctrl+C)",
    )
    parser.add_argument(
        "--cities", default=os.getenv("CITIES", "Quezon City,Manila,Cebu,Davao"),
        help="Comma-separated city names (default: Quezon City,Manila,Cebu,Davao)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key = os.getenv("WEATHERAPI_KEY", "").strip()
    if not api_key:
        logger.error("WEATHERAPI_KEY is not set. Copy .env.example to .env and add your key.")
        return 1
    if args.interval <= 0:
        logger.error("--interval must be greater than 0.")
        return 1
    cities = [c.strip() for c in args.cities.split(",") if c.strip()]
    if not cities:
        logger.error("No cities configured.")
        return 1

    streamer = StreamingDataProducer(args.bootstrap_servers, args.topic, api_key, cities)

    def handle_signal(signum, _frame):
        logger.info("Received signal %s, shutting down...", signum)
        streamer.stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    return streamer.produce_stream(interval=args.interval, duration=args.duration)


if __name__ == "__main__":
    sys.exit(main())
