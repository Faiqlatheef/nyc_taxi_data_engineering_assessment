import hashlib, pandas as pd

PAYMENT_MAP={0:"Flex Fare",1:"Credit card",2:"Cash",3:"No charge",4:"Dispute",5:"Unknown",6:"Voided trip"}

def clean(df):
    df=df.copy()
    required=["tpep_pickup_datetime","tpep_dropoff_datetime","PULocationID","DOLocationID",
              "trip_distance","fare_amount","total_amount","payment_type"]
    missing=[c for c in required if c not in df]
    if missing: raise ValueError(f"Missing columns: {missing}")
    df["tpep_pickup_datetime"]=pd.to_datetime(df["tpep_pickup_datetime"],errors="coerce")
    df["tpep_dropoff_datetime"]=pd.to_datetime(df["tpep_dropoff_datetime"],errors="coerce")
    for c in ["passenger_count","trip_distance","fare_amount","extra","mta_tax","tip_amount",
              "tolls_amount","improvement_surcharge","total_amount","congestion_surcharge","airport_fee"]:
        if c in df: df[c]=pd.to_numeric(df[c],errors="coerce")
    df["trip_duration_minutes"]=(df.tpep_dropoff_datetime-df.tpep_pickup_datetime).dt.total_seconds()/60
    valid=(df.tpep_pickup_datetime.notna() & df.tpep_dropoff_datetime.notna() &
           df.trip_duration_minutes.gt(0) & df.trip_duration_minutes.le(1440) &
           df.trip_distance.notna() & df.trip_distance.ge(0) &
           df.fare_amount.notna() & df.fare_amount.ge(0) &
           df.total_amount.notna() & df.total_amount.ge(0) &
           df.PULocationID.between(1,264) & df.DOLocationID.between(1,264) &
           df.payment_type.notna())
    if "passenger_count" in df:
        valid &= df.passenger_count.isna() | df.passenger_count.between(0,9)
    good=df.loc[valid].copy(); bad=df.loc[~valid].copy()
    def make_id(r):
        s="|".join(map(str,[r.get("VendorID"),r.tpep_pickup_datetime,r.tpep_dropoff_datetime,
                             r.PULocationID,r.DOLocationID,r.fare_amount,r.total_amount]))
        return hashlib.sha256(s.encode()).hexdigest()
    good["trip_id"]=good.apply(make_id,axis=1)
    good["pickup_date"]=good.tpep_pickup_datetime.dt.date
    good["pickup_hour"]=good.tpep_pickup_datetime.dt.hour
    good["source_month"]=good.tpep_pickup_datetime.dt.strftime("%Y-%m")
    return good,bad,len(df)
