import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import psycopg
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.structs import TopicPartition
from prometheus_client import Counter, Gauge, Histogram, start_http_server

TOPIC = "taxi-trips-stream"
GROUP = "taxi-stream-processor"

RECEIVED = Counter("taxi_events_received_total", "Events received")
PROCESSED = Counter("taxi_events_processed_total", "Events processed")
FAILED = Counter("taxi_events_failed_total", "Events failed")
RAW = Counter("taxi_raw_records_written_total", "Raw rows written")
AGG = Counter("taxi_aggregate_records_written_total", "Aggregate rows written")
EPS = Gauge("taxi_events_per_second", "Recent processing throughput")
LAG = Gauge("taxi_consumer_lag", "Estimated consumer lag", ["partition"])
LAT = Histogram("taxi_processing_latency_seconds", "Processing latency")

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}'
)
log = logging.getLogger("stream")

DDL = """
CREATE TABLE IF NOT EXISTS taxi_raw_events(
 event_id TEXT PRIMARY KEY,
 event_time TIMESTAMPTZ NOT NULL,
 dropoff_time TIMESTAMPTZ,
 pickup_location_id INT NOT NULL,
 dropoff_location_id INT NOT NULL,
 payment_type INT,
 trip_distance DOUBLE PRECISION,
 fare_amount DOUBLE PRECISION,
 total_amount DOUBLE PRECISION,
 ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS taxi_window_aggregates(
 window_start TIMESTAMPTZ NOT NULL,
 window_end TIMESTAMPTZ NOT NULL,
 pickup_location_id INT NOT NULL,
 trip_count BIGINT NOT NULL,
 total_revenue DOUBLE PRECISION NOT NULL,
 average_fare DOUBLE PRECISION NOT NULL,
 updated_at TIMESTAMPTZ DEFAULT now(),
 PRIMARY KEY(window_start, window_end, pickup_location_id)
);
"""

def conn():
    return psycopg.connect(
        host=os.getenv("STREAM_DB_HOST", "localhost"),
        port=os.getenv("STREAM_DB_PORT", "5433"),
        dbname=os.getenv("STREAM_DB_NAME", "taxi_stream"),
        user=os.getenv("STREAM_DB_USER", "taxi"),
        password=os.getenv("STREAM_DB_PASSWORD", "taxi"),
    )

def init_db():
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(DDL)
        c.commit()

def ensure_topic(bootstrap):
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap,
        client_id="taxi-assessment-admin"
    )
    try:
        if TOPIC not in admin.list_topics():
            admin.create_topics([
                NewTopic(name=TOPIC, num_partitions=6, replication_factor=3)
            ])
            log.info("created topic=%s partitions=6 replication_factor=3", TOPIC)
    finally:
        admin.close()

def dt(value):
    x = datetime.fromisoformat(value)
    return x if x.tzinfo else x.replace(tzinfo=timezone.utc)

def process(x):
    event = dt(x["event_time"])
    minute = event.replace(second=0, microsecond=0)

    # Five-minute sliding window with a one-minute slide.
    # One event contributes to five overlapping windows.
    starts = [minute - timedelta(minutes=i) for i in range(5)]

    with conn() as c:
        with c.cursor() as cur:
            cur.execute(
                """INSERT INTO taxi_raw_events(
                    event_id,event_time,dropoff_time,pickup_location_id,
                    dropoff_location_id,payment_type,trip_distance,
                    fare_amount,total_amount)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(event_id) DO NOTHING""",
                (
                    x["event_id"], event, dt(x["dropoff_time"]),
                    x["pickup_location_id"], x["dropoff_location_id"],
                    x.get("payment_type"), x.get("trip_distance"),
                    x.get("fare_amount"), x.get("total_amount")
                )
            )

            # Idempotency: duplicate Kafka deliveries do not affect aggregates.
            if cur.rowcount == 0:
                c.commit()
                return False

            for start in starts:
                end = start + timedelta(minutes=5)
                cur.execute(
                    """INSERT INTO taxi_window_aggregates(
                        window_start,window_end,pickup_location_id,
                        trip_count,total_revenue,average_fare)
                    VALUES(%s,%s,%s,1,%s,%s)
                    ON CONFLICT(window_start,window_end,pickup_location_id)
                    DO UPDATE SET
                        trip_count = taxi_window_aggregates.trip_count + 1,
                        total_revenue = taxi_window_aggregates.total_revenue
                                      + EXCLUDED.total_revenue,
                        average_fare = (
                            taxi_window_aggregates.total_revenue
                            + EXCLUDED.total_revenue
                        ) / (taxi_window_aggregates.trip_count + 1),
                        updated_at = now()""",
                    (
                        start, end, x["pickup_location_id"],
                        x["total_amount"], x["fare_amount"]
                    )
                )
        c.commit()

    return True

def update_lag(consumer, message):
    try:
        tp = TopicPartition(message.topic, message.partition)
        end_offset = consumer.end_offsets([tp]).get(tp, 0)
        current = consumer.position(tp)
        LAG.labels(str(message.partition)).set(
            max(0, end_offset - current)
        )
    except Exception:
        pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9094")
    parser.add_argument("--init-only", action="store_true")
    parser.add_argument("--metrics-port", type=int, default=8000)
    args = parser.parse_args()

    init_db()

    if args.init_only:
        ensure_topic(args.bootstrap)
        print("database and Kafka topic initialized")
        return

    ensure_topic(args.bootstrap)
    start_http_server(args.metrics_port)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=args.bootstrap,
        group_id=GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=100,
    )

    last = time.time()
    count = 0
    log.info("consumer started")

    try:
        for message in consumer:
            RECEIVED.inc()
            started = time.perf_counter()

            try:
                x = message.value

                # Demonstration-level late-event signal.
                # Production should use watermarks and an explicit allowed-lateness policy.
                if dt(x["event_time"]) < datetime.now(timezone.utc) - timedelta(minutes=2):
                    log.info(
                        "late_event event_id=%s event_time=%s",
                        x["event_id"], x["event_time"]
                    )

                inserted = process(x)

                if inserted:
                    RAW.inc()
                    AGG.inc()
                    PROCESSED.inc()
                    count += 1

                # Acknowledge only after database persistence succeeds.
                consumer.commit()
                update_lag(consumer, message)
                LAT.observe(time.perf_counter() - started)

                elapsed = time.time() - last
                if elapsed >= 5:
                    EPS.set(count / elapsed)
                    count = 0
                    last = time.time()

            except Exception:
                FAILED.inc()
                log.exception(
                    "processing failed; Kafka offset intentionally not committed"
                )
                time.sleep(1)

    finally:
        consumer.close()

if __name__ == "__main__":
    main()
