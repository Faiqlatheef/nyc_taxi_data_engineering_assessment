import argparse, numpy as np, pandas as pd
from common import RAW_DIR

def generate(rows, month):
    rng = np.random.default_rng(42)
    start = pd.Timestamp(f"{month}-01")
    seconds = int((start + pd.offsets.MonthEnd(1) - start).total_seconds())
    pickup = start + pd.to_timedelta(rng.integers(0, seconds, rows), unit="s")
    duration = rng.integers(180, 5400, rows)
    distance = np.round(rng.gamma(2.2, 2.2, rows), 3)
    fare = np.round(3.5 + distance*rng.uniform(2,4.5,rows), 2)
    tip = np.round(np.where(rng.random(rows)<.65, fare*rng.uniform(0,.25,rows), 0),2)
    total = np.round(fare+tip+1.5,2)
    df = pd.DataFrame({
        "VendorID":rng.choice([1,2,6,7],rows),
        "tpep_pickup_datetime":pickup,
        "tpep_dropoff_datetime":pickup+pd.to_timedelta(duration,unit="s"),
        "passenger_count":rng.integers(1,5,rows),
        "trip_distance":distance,
        "RatecodeID":rng.choice([1,2,3,4,5],rows),
        "store_and_fwd_flag":rng.choice(["N","Y"],rows,p=[.97,.03]),
        "PULocationID":rng.integers(1,264,rows),
        "DOLocationID":rng.integers(1,264,rows),
        "payment_type":rng.choice([1,2,3,4],rows,p=[.7,.25,.03,.02]),
        "fare_amount":fare,"extra":0.0,"mta_tax":.5,"tip_amount":tip,
        "tolls_amount":0.0,"improvement_surcharge":1.0,
        "total_amount":total,"congestion_surcharge":2.5,"airport_fee":0.0})
    path=RAW_DIR/f"yellow_tripdata_{month}.parquet"
    df.to_parquet(path,index=False)
    print(path)

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--rows",type=int,default=50000); p.add_argument("--month",default="2023-01")
    a=p.parse_args(); generate(a.rows,a.month)
