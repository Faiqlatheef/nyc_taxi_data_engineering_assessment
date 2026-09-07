CREATE SCHEMA IF NOT EXISTS taxi;

CREATE TABLE IF NOT EXISTS taxi.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS taxi.dim_time (
    time_key INTEGER PRIMARY KEY,
    hour SMALLINT NOT NULL,
    minute_bucket SMALLINT NOT NULL,
    time_bucket VARCHAR(20) NOT NULL,
    is_peak_hour BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS taxi.dim_location (
    location_key INTEGER PRIMARY KEY,
    taxi_zone_id INTEGER UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS taxi.dim_payment_type (
    payment_type_key INTEGER PRIMARY KEY,
    payment_type_code INTEGER UNIQUE NOT NULL,
    payment_type VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS taxi.fact_taxi_trips (
    trip_id VARCHAR(64) PRIMARY KEY,
    pickup_date_key INTEGER REFERENCES taxi.dim_date(date_key),
    pickup_time_key INTEGER REFERENCES taxi.dim_time(time_key),
    pickup_location_key INTEGER REFERENCES taxi.dim_location(location_key),
    dropoff_location_key INTEGER REFERENCES taxi.dim_location(location_key),
    payment_type_key INTEGER REFERENCES taxi.dim_payment_type(payment_type_key),
    vendor_id INTEGER,
    rate_code_id INTEGER,
    store_and_forward_flag VARCHAR(1),
    passenger_count NUMERIC(5,2),
    trip_distance NUMERIC(12,3) NOT NULL,
    trip_duration_minutes NUMERIC(12,3) NOT NULL,
    fare_amount NUMERIC(14,2) NOT NULL,
    extra NUMERIC(14,2),
    mta_tax NUMERIC(14,2),
    tip_amount NUMERIC(14,2),
    tolls_amount NUMERIC(14,2),
    improvement_surcharge NUMERIC(14,2),
    congestion_surcharge NUMERIC(14,2),
    airport_fee NUMERIC(14,2),
    total_amount NUMERIC(14,2) NOT NULL,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    source_month VARCHAR(7) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_pickup ON taxi.fact_taxi_trips(pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_fact_location ON taxi.fact_taxi_trips(pickup_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_payment ON taxi.fact_taxi_trips(payment_type_key);
