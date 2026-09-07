import os,time,logging
import pandas as pd
from prefect import flow,task,get_run_logger
from common import RAW_DIR,REJECTED_DIR
from download_data import download_month
from transform import clean
from load import init_schema,load_dimensions,load_facts

@task(retries=2,retry_delay_seconds=10)
def extract(month):
    path=download_month(month)
    df = pd.read_parquet(path)
    n = os.getenv("TAXI_MAX_ROWS_PER_FILE")
    return df.head(int(n)) if n else df

@task
def transform(df): return clean(df)

@task
def load(df):
    load_dimensions(df)
    return load_facts(df)

@flow(name="nyc-yellow-taxi-batch-etl")
def taxi_etl(months=None):
    log=get_run_logger(); start=time.time()
    months=months or os.getenv("TAXI_MONTHS","2023-01,2023-02").split(",")
    init_schema(); total=[0,0,0,0]
    try:
        for m in map(str.strip,months):
            raw=extract.submit(m).result()
            good,bad,n=transform.submit(raw).result()
            total[0]+=n; total[1]+=len(good); total[2]+=len(bad)
            if len(bad): bad.to_parquet(REJECTED_DIR/f"rejected_{m}.parquet",index=False)
            loaded=load.submit(good).result(); total[3]+=loaded
            log.info("month=%s rows_read=%s rows_valid=%s rows_rejected=%s rows_loaded=%s",m,n,len(good),len(bad),loaded)
        log.info("SUCCESS read=%s valid=%s rejected=%s loaded=%s duration_seconds=%.2f",*total,time.time()-start)
    except Exception:
        log.exception("ETL failed"); raise

if __name__=="__main__": taxi_etl()
