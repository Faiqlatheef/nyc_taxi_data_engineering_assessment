# Physical Demonstration Notes

1. Explain assumptions before running anything.
2. Show the Assignment 1 star schema.
3. Run the ETL and show read/valid/rejected/loaded counts.
4. Run the three required SQL metrics.
5. Open Streamlit.
6. Start Kafka and explain brokers, partitions and consumer groups.
7. Start consumer then producer.
8. Show raw events and window aggregates.
9. Open Grafana and explain throughput, failures, latency and lag.
10. Explain event time vs processing time, late events, idempotency and backpressure.
11. Finish with the production 50K events/sec architecture.

Be able to explain why the deterministic event/trip ID is used, why dimensions load before facts, why offsets are committed after successful persistence, and why local performance must not be presented as production capacity.
