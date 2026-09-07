import os, requests
from common import RAW_DIR

URL = "https://d37ci6vzurych.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"

def download_month(month):
    path = RAW_DIR / f"yellow_tripdata_{month}.parquet"
    if path.exists() and path.stat().st_size:
        return path
    with requests.get(URL.format(month=month), stream=True, timeout=120) as r:
        r.raise_for_status()
        with path.open("wb") as f:
            for chunk in r.iter_content(1024*1024):
                if chunk: f.write(chunk)
    return path

if __name__ == "__main__":
    for month in os.getenv("TAXI_MONTHS","2023-01,2023-02").split(","):
        print(download_month(month.strip()))
