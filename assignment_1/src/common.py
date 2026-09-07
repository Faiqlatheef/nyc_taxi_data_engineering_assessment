import hashlib, os
from pathlib import Path
import psycopg
from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
REJECTED_DIR = ROOT / "data" / "rejected"
RAW_DIR.mkdir(parents=True, exist_ok=True)
REJECTED_DIR.mkdir(parents=True, exist_ok=True)

def get_database_url():
    return "postgresql+psycopg://{u}:{p}@{h}:{port}/{db}".format(
        u=os.getenv("POSTGRES_USER","taxi"), p=os.getenv("POSTGRES_PASSWORD","taxi"),
        h=os.getenv("POSTGRES_HOST","localhost"), port=os.getenv("POSTGRES_PORT","5432"),
        db=os.getenv("POSTGRES_DB","taxi_dw"))

def get_engine():
    return create_engine(get_database_url(), pool_pre_ping=True)

def configure_logging():
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='{"time":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}')
    return logging.getLogger("taxi_etl")

def trip_id(row):
    raw = "|".join(map(str, [
        row.get("VendorID"), row.get("tpep_pickup_datetime"),
        row.get("tpep_dropoff_datetime"), row.get("PULocationID"),
        row.get("DOLocationID"), row.get("fare_amount"), row.get("total_amount")]))
    return hashlib.sha256(raw.encode()).hexdigest()
