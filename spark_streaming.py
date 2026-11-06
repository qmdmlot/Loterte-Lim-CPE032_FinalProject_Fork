"""Spark Structured Streaming job: Kafka -> parse/validate -> MongoDB Atlas.

Writes two things:
  * readings      raw validated messages (idempotent upserts)
  * readings_agg  1-minute and 5-minute windowed avg/min/max/count per sensor+metric
"""
import logging
import os
import signal
import sys
import time

import pyspark
from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "streaming-data")
STARTING_OFFSETS = os.getenv("SPARK_STARTING_OFFSETS", "latest")
MONGODB_URI = os.getenv("MONGODB_URI", "")
MONGODB_DB = os.getenv("MONGODB_DB", "streaming_db")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "./checkpoints")
TRIGGER = os.getenv("SPARK_TRIGGER", "10 seconds")
AGG_WINDOWS_MINUTES = (1, 5)

KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"

SCHEMA = StructType(
    [
        StructField("timestamp", StringType()),
        StructField("value", DoubleType()),
        StructField("metric_type", StringType()),
        StructField("sensor_id", StringType()),
    ]
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spark_streaming")

_client = None


def mongo_db():
    """Lazily created MongoDB handle. foreachBatch callbacks run on the driver."""
    global _client
    if _client is None:
        if not MONGODB_URI:
            raise RuntimeError("MONGODB_URI is not set. Copy .env.example to .env and fill it in.")
        _client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10000)
    return _client[MONGODB_DB]


def ensure_indexes():
    db = mongo_db()
    db["readings"].create_index(
        [("sensor_id", ASCENDING), ("metric_type", ASCENDING), ("timestamp", ASCENDING)], unique=True
    )
    db["readings"].create_index([("metric_type", ASCENDING), ("timestamp", ASCENDING)])
    db["readings_agg"].create_index(
        [
            ("sensor_id", ASCENDING),
            ("metric_type", ASCENDING),
            ("window_minutes", ASCENDING),
            ("window_start", ASCENDING),
        ],
        unique=True,
    )
    log.info("MongoDB indexes ensured on database '%s'.", MONGODB_DB)


def bulk_upsert(collection: str, ops: list, what: str):
    """Write with retries. If all retries fail, raise so Spark does not advance the checkpoint."""
    if not ops:
        return
    for attempt in range(1, 4):
        try:
            mongo_db()[collection].bulk_write(ops, ordered=False)
            return
        except PyMongoError as exc:
            log.warning("MongoDB write failed for %s (attempt %d/3): %s", what, attempt, exc)
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)


def write_raw(batch_df, batch_id):
    try:
        rows = batch_df.select("event_time", "value", "metric_type", "sensor_id").collect()
        ops = [
            UpdateOne(
                {"sensor_id": r["sensor_id"], "metric_type": r["metric_type"], "timestamp": r["event_time"]},
                {"$set": {"value": float(r["value"])}},
                upsert=True,
            )
            for r in rows
        ]
        bulk_upsert("readings", ops, f"raw batch {batch_id}")
        if rows:
            log.info("Batch %s: wrote %d raw readings.", batch_id, len(rows))
    except Exception:
        log.exception("Raw batch %s failed.", batch_id)
        raise


def make_agg_writer(minutes: int):
    def write_agg(batch_df, batch_id):
        try:
            rows = batch_df.collect()
            ops = [
                UpdateOne(
                    {
                        "sensor_id": r["sensor_id"],
                        "metric_type": r["metric_type"],
                        "window_minutes": minutes,
                        "window_start": r["window"]["start"],
                    },
                    {
                        "$set": {
                            "window_end": r["window"]["end"],
                            "avg": round(float(r["avg_value"]), 4),
                            "min": float(r["min_value"]),
                            "max": float(r["max_value"]),
                            "count": int(r["n"]),
                        }
                    },
                    upsert=True,
                )
                for r in rows
            ]
            bulk_upsert("readings_agg", ops, f"{minutes}-min aggregates batch {batch_id}")
            if rows:
                log.info("Batch %s: upserted %d %d-minute windows.", batch_id, len(rows), minutes)
        except Exception:
            log.exception("%d-minute aggregate batch %s failed.", minutes, batch_id)
            raise

    return write_agg


def build_spark():
    spark = (
        SparkSession.builder.appName("WeatherStreamPipeline")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")  # collected datetimes are naive UTC
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_stream(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        # malformed JSON or missing fields become nulls and are dropped here
        .dropna(subset=["event_time", "value", "metric_type", "sensor_id"])
    )
    return parsed


def aggregate(parsed, minutes: int):
    return (
        parsed.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", f"{minutes} minutes"), "sensor_id", "metric_type")
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count("*").alias("n"),
        )
    )


def main() -> int:
    try:
        ensure_indexes()
    except Exception as exc:
        log.error("Cannot reach MongoDB Atlas: %s", exc)
        log.error("Check MONGODB_URI and that your IP is in the Atlas Network Access list.")
        return 1

    spark = build_spark()
    parsed = parse_stream(spark)
    queries = []

    queries.append(
        parsed.writeStream.foreachBatch(write_raw)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "raw"))
        .trigger(processingTime=TRIGGER)
        .start()
    )
    for minutes in AGG_WINDOWS_MINUTES:
        queries.append(
            aggregate(parsed, minutes)
            .writeStream.outputMode("update")
            .foreachBatch(make_agg_writer(minutes))
            .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, f"agg_{minutes}m"))
            .trigger(processingTime=TRIGGER)
            .start()
        )
    log.info("Streaming started: %d queries reading topic '%s' from %s.", len(queries), TOPIC, BOOTSTRAP)

    def shutdown(signum=None, _frame=None):
        log.info("Stopping streaming queries...")
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass

    signal.signal(signal.SIGTERM, shutdown)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        shutdown()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
