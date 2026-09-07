import argparse,json,time,pandas as pd
from kafka import KafkaProducer
TOPIC="taxi-trips-stream"

def payload(r):
    pickup=pd.Timestamp(r["tpep_pickup_datetime"]); dropoff=pd.Timestamp(r["tpep_dropoff_datetime"])
    event_id="|".join(map(str,[r.get("VendorID"),pickup.isoformat(),dropoff.isoformat(),r.get("PULocationID"),r.get("DOLocationID"),r.get("fare_amount"),r.get("total_amount")]))
    return {"event_id":event_id,"event_time":pickup.isoformat(),"dropoff_time":dropoff.isoformat(),
            "pickup_location_id":int(r.get("PULocationID",0)),"dropoff_location_id":int(r.get("DOLocationID",0)),
            "payment_type":int(r.get("payment_type",0)),"trip_distance":float(r.get("trip_distance",0) or 0),
            "fare_amount":float(r.get("fare_amount",0) or 0),"total_amount":float(r.get("total_amount",0) or 0)}

p=argparse.ArgumentParser(); p.add_argument("--input",required=True); p.add_argument("--rate",type=float,default=25); p.add_argument("--bootstrap",default="localhost:9094"); p.add_argument("--limit",type=int,default=0); a=p.parse_args()
if not 10<=a.rate<=50: raise ValueError("rate must be 10-50 events/sec")
producer=KafkaProducer(bootstrap_servers=a.bootstrap,value_serializer=lambda v:json.dumps(v).encode(),key_serializer=lambda v:str(v).encode(),acks="all",retries=5,linger_ms=10)
df=pd.read_parquet(a.input)
if a.limit: df=df.head(a.limit)
for i,(_,r) in enumerate(df.iterrows(),1):
    x=payload(r); producer.send(TOPIC,key=x["pickup_location_id"],value=x)
    if i%100==0: producer.flush(); print("sent",i)
    time.sleep(1/a.rate)
producer.flush(); producer.close(); print("complete")
