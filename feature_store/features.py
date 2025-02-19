from feast import StreamFeatureView, Field, Entity
from feast.types import Float32, Int64, String  # Use feast.types instead of ValueType
from datetime import timedelta
from data_sources import realtime_trips_stream, location_updates_stream
from entities import location, trip  # Import entities from entities.py

# Define the stream feature view
trip_stream_view = StreamFeatureView(
    name="realtime_trips_stream",
    entities=[trip],  # Use the imported entity
    ttl=timedelta(days=1),
    schema=[
        Field(name="vendor_id", dtype=Int64),
        Field(name="passenger_count", dtype=Int64),
        Field(name="trip_distance", dtype=Float32),
        Field(name="fare_amount", dtype=Float32),
        Field(name="tip_amount", dtype=Float32),
        Field(name="total_amount", dtype=Float32),
        Field(name="rate_code_id", dtype=Int64),
        Field(name="pickup_latitude", dtype=Float32),
        Field(name="pickup_longitude", dtype=Float32),
        Field(name="dropoff_latitude", dtype=Float32),
        Field(name="dropoff_longitude", dtype=Float32),
        Field(name="payment_type", dtype=Int64)
    ],
    source=realtime_trips_stream,
    online=True,
    description="Real-time taxi trip features"
)

# Define the location stream feature view
location_stream_view = StreamFeatureView(
    name="location_updates_stream",
    entities=[location],  # Use the imported entity
    ttl=timedelta(hours=24),
    schema=[
        Field(name="latitude", dtype=Float32),
        Field(name="longitude", dtype=Float32),
        Field(name="current_demand", dtype=Int64),
        Field(name="available_taxis", dtype=Int64),
        Field(name="surge_multiplier", dtype=Float32),
        Field(name="weather_condition", dtype=String),
        Field(name="traffic_level", dtype=String)
    ],
    source=location_updates_stream,
    online=True,
    description="Real-time location-based features"
)