# Assignment 2 - Real-Time Streaming

## Stack
Apache Kafka (3 KRaft brokers), Python producer/consumer, PostgreSQL, Prometheus and Grafana.

## Start
```bash
pip install -r requirements.txt
docker compose up -d
python consumer/stream_processor.py --init-only
python consumer/stream_processor.py
```

In another terminal:
```bash
python producer/producer.py --input ../assignment_1/data/raw/yellow_tripdata_2023-01.parquet --rate 25
```

The consumer creates `taxi-trips-stream` with 6 partitions and replication factor 3 if it does not exist.

## Ports
- Kafka: localhost:9094
- PostgreSQL: localhost:5433
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
- Grafana login: admin / admin

## Streaming semantics
The processor uses event time and a 5-minute sliding window with a 1-minute slide, so each event contributes to five overlapping windows. Raw events are idempotent via `event_id`. Kafka offsets are committed only after successful persistence.

For production, use watermarks/allowed lateness, retry and dead-letter topics, idempotent sinks, batch/bulk writes and a high-throughput analytical store.
