SELECT 
    location_id,
    latitude,
    longitude,
    current_demand,
    available_taxis,
    surge_multiplier,
    weather_condition,
    traffic_level,
    event_timestamp
FROM feature_store.location_features
WHERE event_timestamp >= NOW() - INTERVAL '24 hours'  -- Adjust time window as needed
  AND location_id IS NOT NULL
  AND latitude BETWEEN -90 AND 90
  AND longitude BETWEEN -180 AND 180
  AND current_demand >= 0
  AND available_taxis >= 0
  AND surge_multiplier >= 1.0
ORDER BY event_timestamp DESC 