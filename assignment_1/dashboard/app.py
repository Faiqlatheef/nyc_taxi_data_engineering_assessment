import os,pandas as pd,streamlit as st
from sqlalchemy import create_engine
st.set_page_config(page_title="NYC Taxi Analytics",layout="wide")
st.title("NYC Yellow Taxi - Batch Analytics")
url=f"postgresql+psycopg://{os.getenv('POSTGRES_USER','taxi')}:{os.getenv('POSTGRES_PASSWORD','taxi')}@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','taxi_dw')}"
e=create_engine(url)
try:
    s=pd.read_sql("SELECT COUNT(*) trips,SUM(total_amount) revenue,AVG(trip_distance) avg_distance,SUM(total_amount)/NULLIF(SUM(trip_distance),0) fare_per_mile FROM taxi.fact_taxi_trips",e)
    h=pd.read_sql("SELECT dt.hour,COUNT(*) trip_count FROM taxi.fact_taxi_trips f JOIN taxi.dim_time dt ON f.pickup_time_key=dt.time_key GROUP BY dt.hour ORDER BY dt.hour",e)
    p=pd.read_sql("SELECT p.payment_type,SUM(f.total_amount) revenue FROM taxi.fact_taxi_trips f JOIN taxi.dim_payment_type p ON f.payment_type_key=p.payment_type_key GROUP BY p.payment_type ORDER BY revenue DESC",e)
    a,b,c,d=st.columns(4); a.metric("Trips",f"{int(s.trips.iloc[0]):,}"); b.metric("Revenue",f"${s.revenue.iloc[0]:,.2f}"); c.metric("Avg Distance",f"{s.avg_distance.iloc[0]:.2f} mi"); d.metric("Fare/Mile",f"${s.fare_per_mile.iloc[0]:.2f}")
    st.subheader("Trips by Pickup Hour"); st.line_chart(h.set_index("hour"))
    st.subheader("Revenue by Payment Type"); st.bar_chart(p.set_index("payment_type"))
except Exception as ex: st.error(f"Database not ready or ETL has not run: {ex}")
