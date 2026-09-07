# NYC Taxi Data Engineering Assessment

Senior/Lead submission covering both assignments.

## Assumptions
- Source: NYC TLC Yellow Taxi monthly Parquet files.
- Assignment 1: Prefect + PostgreSQL + Pandas/PyArrow + Streamlit.
- Assignment 2: 3-broker Kafka KRaft + Python streaming + PostgreSQL + Prometheus + Grafana.
- Default TLC months: January and February 2023.
- A synthetic-data fallback is included for constrained/offline demonstrations.
- Local resources are intentionally modest; production scaling is documented separately.
- Event time is used for streaming logic; late-event handling is bounded and production watermarking is documented.

## Repository
- `assignment_1/` batch ETL and dashboard
- `assignment_2/` Kafka streaming and observability
- `docs/` architecture and demo notes

## Quick start
Assignment 1:
```bash
cd assignment_1
./setup.sh
source .venv/bin/activate
python src/generate_sample_data.py --rows 50000
python src/flow.py
streamlit run dashboard/app.py
```

Real TLC data:
```bash
export TAXI_MONTHS="2023-01,2023-02"
python src/download_data.py
python src/flow.py
```

Assignment 2:
```bash
cd assignment_2
pip install -r requirements.txt
docker compose up -d
python consumer/stream_processor.py --init-only
python consumer/stream_processor.py
```
Then, in another terminal:
```bash
python producer/producer.py --input ../assignment_1/data/raw/yellow_tripdata_2023-01.parquet --rate 25
```
