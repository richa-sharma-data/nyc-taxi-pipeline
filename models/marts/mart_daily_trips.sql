-- models/marts/mart_daily_trips.sql

WITH trips AS (
    SELECT * FROM {{ ref('stg_yellow_taxi') }}
)

SELECT
    DATE(pickup_at)                         AS trip_date,
    COUNT(*)                                AS total_trips,
    ROUND(SUM(total_amount), 2)             AS total_revenue,
    ROUND(AVG(total_amount), 2)             AS avg_fare,
    ROUND(AVG(trip_distance), 2)            AS avg_distance_miles,
    ROUND(AVG(trip_duration_mins), 1)       AS avg_duration_mins,
    ROUND(AVG(tip_amount), 2)               AS avg_tip,
    SUM(passenger_count)                    AS total_passengers

FROM trips
GROUP BY DATE(pickup_at)
ORDER BY trip_date
