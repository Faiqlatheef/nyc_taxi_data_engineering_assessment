# Production Architecture - 50,000 Events/Second

Local Docker is a functional demonstration, not a production capacity claim.

```text
Producers -> Load Balancer -> Managed Kafka
                               |
                         partitioned topic
                               |
                    Kubernetes consumer group
                         /                                     /                               Raw object store      OLAP store
                        \               /
                         -> BI / APIs / ML
```

## Scaling
- Increase Kafka partitions based on measured producer/consumer throughput.
- Use replication factor >= 3 and min ISR >= 2.
- Scale consumer replicas horizontally in one consumer group.
- Batch database writes rather than opening one transaction per event.
- Use an OLAP-oriented sink for sustained analytical workloads.
- Autoscale from consumer lag, throughput, CPU and latency.

## Event ordering
Use event time rather than processing time. Production stream processors should use watermarks and allowed lateness. Late events beyond the policy should enter a correction/reconciliation path.

## Delivery semantics
Use deterministic event IDs and idempotent sink writes. Commit Kafka offsets only after successful persistence. Add retry and dead-letter topics for bounded failure handling.

## Observability
Track throughput, processing latency, consumer lag, error rate, database latency, Kafka broker health, partition skew and data-quality metrics.

## Security
Use TLS, secret management, least-privilege IAM, private networking, encryption at rest and audit logging.

## Cloud mapping
The architecture can map to GCP using managed Kafka or Kafka-compatible infrastructure, GKE, Cloud Storage, Cloud SQL/AlloyDB and an analytical warehouse/OLAP engine. Exact services should be chosen based on enterprise standards, residency and cost.
