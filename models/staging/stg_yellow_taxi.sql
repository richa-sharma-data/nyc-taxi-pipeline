-- models/staging/stg_yellow_taxi.sql

WITH source AS (
    SELECT * FROM taxi_staging.yellow_taxi_raw
),

cleaned AS (
    SELECT
        -- Identifiers
        VendorID                                    AS vendor_id,

        -- Timestamps
        tpep_pickup_datetime                        AS pickup_at,
        tpep_dropoff_datetime                       AS dropoff_at,

        -- Derived time fields
        DATEDIFF('minute', tpep_pickup_datetime,
                            tpep_dropoff_datetime)  AS trip_duration_mins,

        -- Trip details
        passenger_count,
        trip_distance,
        PULocationID                                AS pickup_location_id,
        DOLocationID                                AS dropoff_location_id,
        RatecodeID                                  AS rate_code_id,

        -- Payments
        payment_type,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        congestion_surcharge,

        -- Metadata
        ingestion_month,
        ingested_at

    FROM source

    WHERE
        tpep_pickup_datetime  IS NOT NULL
        AND tpep_dropoff_datetime IS NOT NULL
        AND trip_distance         > 0
        AND total_amount          > 0
        AND passenger_count       > 0
        and EXTRACT(year from tpep_pickup_datetime) in (2025, 2026)
)

SELECT * FROM cleaned
