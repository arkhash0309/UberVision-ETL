CREATE or REPLACE TABLE 'ubervision-etl.tbl_analytics' AS (
    SELECT
    f.trip_id,
    f.VendorID,
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    p.passenger_count,
    t.trip_distance,
    r.rate_code_name,
    pick.pickup_latitude,
    pick.pickup_longitude,
    drop.dropoff_latitude,
    drop.dropoff_longitude,
    pay.payment_type_name,
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.total_amount

    fare_amount
    'ubervision-etl.fact_table' f 
    JOIN `ubervision-etl.datetime_dimension` d  ON f.datetime_id=d.datetime_id
    JOIN `ubervision-etl.passenger_count_dimension` p  ON p.passenger_id=f.passenger_id  
    JOIN `ubervision-etl.trip_distance_dimension` t  ON t.trip_distance_id=f.trip_distance_id  
    JOIN `ubervision-etl.rate_code_dimension` r ON r.rate_code_id=f.rate_code_id  
    JOIN `ubervision-etl.pickup_location_dimension` pick ON pick.pickup_location_id=f.pickup_location_id
    JOIN `ubervision-etl.dropoff_location_dimension` drop ON drop.dropoff_location_id=f.dropoff_location_id
    JOIN `ubervision-etl.payment_type_dimension` pay ON pay.payment_type_id=f.payment_type_id
);