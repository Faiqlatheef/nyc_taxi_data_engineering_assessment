-- Average fare amount per mile: ratio of aggregates avoids giving a one-mile trip the same weight as a 20-mile trip.
SELECT ROUND(SUM(fare_amount)/NULLIF(SUM(trip_distance),0),2) AS average_fare_per_mile
FROM taxi.fact_taxi_trips WHERE trip_distance > 0;

-- Peak hours
SELECT dt.hour, COUNT(*) AS trip_count
FROM taxi.fact_taxi_trips f JOIN taxi.dim_time dt ON f.pickup_time_key=dt.time_key
GROUP BY dt.hour ORDER BY trip_count DESC;

-- Total revenue by payment type
SELECT p.payment_type, ROUND(SUM(f.total_amount),2) AS total_revenue
FROM taxi.fact_taxi_trips f JOIN taxi.dim_payment_type p ON f.payment_type_key=p.payment_type_key
GROUP BY p.payment_type ORDER BY total_revenue DESC;
