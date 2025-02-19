SELECT 
    trip_id,
    vendor_id,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    rate_code_id,
    event_timestamp,
    pickup_latitude,
    pickup_longitude,
    dropoff_latitude,
    dropoff_longitude,
    payment_type
FROM feature_store.trip_features
WHERE event_timestamp >= NOW() - INTERVAL '90 days'  -- Changed to use event_timestamp
  AND trip_id IS NOT NULL
  AND vendor_id IS NOT NULL
  AND passenger_count > 0
  AND trip_distance >= 0
  AND fare_amount >= 0
ORDER BY event_timestamp DESC 