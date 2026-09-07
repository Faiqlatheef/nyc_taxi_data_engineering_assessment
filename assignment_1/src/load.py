from pathlib import Path
import pandas as pd
from sqlalchemy import text
from common import get_engine

PAYMENT_MAP={0:"Flex Fare",1:"Credit card",2:"Cash",3:"No charge",4:"Dispute",5:"Unknown",6:"Voided trip"}

def init_schema():
    engine=get_engine()
    for stmt in (Path(__file__).resolve().parents[1]/"schema.sql").read_text().split(";"):
        if stmt.strip():
            with engine.begin() as c: c.execute(text(stmt))

def load_dimensions(df):
    e=get_engine()
    d=pd.DataFrame({"date_key":pd.to_datetime(df.pickup_date).dt.strftime("%Y%m%d").astype(int),
                    "full_date":pd.to_datetime(df.pickup_date).dt.date}).drop_duplicates()
    x=pd.to_datetime(d.full_date)
    d["year"]=x.dt.year; d["quarter"]=x.dt.quarter; d["month"]=x.dt.month
    d["month_name"]=x.dt.month_name(); d["day"]=x.dt.day; d["day_of_week"]=x.dt.dayofweek
    d["day_name"]=x.dt.day_name(); d["is_weekend"]=d.day_of_week>=5
    t=pd.DataFrame({"hour":sorted(df.pickup_hour.unique())}); t["minute_bucket"]=0
    t["time_key"]=t.hour*100
    t["time_bucket"]=t.hour.map(lambda h:"Night" if h<6 else "Morning" if h<12 else "Afternoon" if h<18 else "Evening")
    t["is_peak_hour"]=t.hour.isin([7,8,9,16,17,18,19])
    z=pd.concat([df[["PULocationID"]].rename(columns={"PULocationID":"taxi_zone_id"}),
                 df[["DOLocationID"]].rename(columns={"DOLocationID":"taxi_zone_id"})]).drop_duplicates()
    z["location_key"]=z.taxi_zone_id.astype(int)
    p=pd.DataFrame({"payment_type_code":sorted(df.payment_type.astype(int).unique())})
    p["payment_type_key"]=p.payment_type_code; p["payment_type"]=p.payment_type_code.map(PAYMENT_MAP).fillna("Unknown")
    d.to_sql("dim_date_stage",e,schema="taxi",if_exists="replace",index=False)
    t.to_sql("dim_time_stage",e,schema="taxi",if_exists="replace",index=False)
    z.to_sql("dim_location_stage",e,schema="taxi",if_exists="replace",index=False)
    p.to_sql("dim_payment_stage",e,schema="taxi",if_exists="replace",index=False)
    with e.begin() as c:
        c.execute(text("INSERT INTO taxi.dim_date SELECT * FROM taxi.dim_date_stage ON CONFLICT(date_key) DO NOTHING"))
        c.execute(text("INSERT INTO taxi.dim_time SELECT * FROM taxi.dim_time_stage ON CONFLICT(time_key) DO NOTHING"))
        c.execute(text("INSERT INTO taxi.dim_location SELECT location_key,taxi_zone_id FROM taxi.dim_location_stage ON CONFLICT(location_key) DO NOTHING"))
        c.execute(text("INSERT INTO taxi.dim_payment_type SELECT * FROM taxi.dim_payment_stage ON CONFLICT(payment_type_key) DO UPDATE SET payment_type=EXCLUDED.payment_type"))

def load_facts(df):
    if df.empty:return 0
    e=get_engine()
    f=pd.DataFrame({
        "trip_id":df.trip_id,
        "pickup_date_key":pd.to_datetime(df.pickup_date).dt.strftime("%Y%m%d").astype(int),
        "pickup_time_key":df.pickup_hour.astype(int)*100,
        "pickup_location_key":df.PULocationID.astype(int),"dropoff_location_key":df.DOLocationID.astype(int),
        "payment_type_key":df.payment_type.astype(int),"vendor_id":df.get("VendorID"),"rate_code_id":df.get("RatecodeID"),
        "store_and_forward_flag":df.get("store_and_fwd_flag"),"passenger_count":df.get("passenger_count"),
        "trip_distance":df.trip_distance,"trip_duration_minutes":df.trip_duration_minutes,"fare_amount":df.fare_amount,
        "extra":df.get("extra"),"mta_tax":df.get("mta_tax"),"tip_amount":df.get("tip_amount"),
        "tolls_amount":df.get("tolls_amount"),"improvement_surcharge":df.get("improvement_surcharge"),
        "congestion_surcharge":df.get("congestion_surcharge"),"airport_fee":df.get("airport_fee"),
        "total_amount":df.total_amount,"pickup_datetime":df.tpep_pickup_datetime,
        "dropoff_datetime":df.tpep_dropoff_datetime,"source_month":df.source_month})
    f.to_sql("fact_stage",e,schema="taxi",if_exists="replace",index=False,method="multi",chunksize=5000)
    with e.begin() as c:
        r=c.execute(text("""INSERT INTO taxi.fact_taxi_trips(
            trip_id,pickup_date_key,pickup_time_key,pickup_location_key,dropoff_location_key,
            payment_type_key,vendor_id,rate_code_id,store_and_forward_flag,passenger_count,
            trip_distance,trip_duration_minutes,fare_amount,extra,mta_tax,tip_amount,tolls_amount,
            improvement_surcharge,congestion_surcharge,airport_fee,total_amount,pickup_datetime,
            dropoff_datetime,source_month)
        SELECT trip_id,pickup_date_key,pickup_time_key,pickup_location_key,dropoff_location_key,
            payment_type_key,vendor_id,rate_code_id,store_and_forward_flag,passenger_count,
            trip_distance,trip_duration_minutes,fare_amount,extra,mta_tax,tip_amount,tolls_amount,
            improvement_surcharge,congestion_surcharge,airport_fee,total_amount,pickup_datetime,
            dropoff_datetime,source_month
        FROM taxi.fact_stage
        ON CONFLICT(trip_id) DO NOTHING"""))
    return r.rowcount or 0
