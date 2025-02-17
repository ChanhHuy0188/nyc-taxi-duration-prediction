from datetime import timedelta

from data_sources import (
    historical_trips_source,
    location_stats_source,
    realtime_trips_stream,
    location_updates_stream,
)
from entities import (
    trip,
    location,
    vendor,
    payment_type,
    rate_code,
    pickup_location,
    dropoff_location,
)
from feast import Field, FeatureView
from feast.stream_feature_view import stream_feature_view
from feast.types import Float32, Int32, Int64, String
from pyspark.sql import DataFrame


@stream_feature_view(
    entities=[location],
    ttl=timedelta(hours=1),
    mode="spark",
    schema=[
        Field(name="location_id", dtype=Int64),
        Field(name="latitude", dtype=Float32),
        Field(name="longitude", dtype=Float32),
        Field(name="current_demand", dtype=Int32),
        Field(name="available_taxis", dtype=Int32),
        Field(name="surge_multiplier", dtype=Float32),
        Field(name="weather_condition", dtype=String),
        Field(name="traffic_level", dtype=String),
        Field(name="event_timestamp", dtype=String),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=location_updates_stream,
    description="Real-time location-based features",
)
def location_features_stream(df: DataFrame):
    """Process real-time location updates"""
    return df


@stream_feature_view(
    entities=[trip, pickup_location, dropoff_location, vendor],
    ttl=timedelta(hours=2),
    mode="spark",
    schema=[
        Field(name="trip_id", dtype=String),
        Field(name="vendor_id", dtype=Int32),
        Field(name="pickup_datetime", dtype=String),
        Field(name="dropoff_datetime", dtype=String),
        Field(name="passenger_count", dtype=Int32),
        Field(name="trip_distance", dtype=Float32),
        Field(name="pickup_longitude", dtype=Float32),
        Field(name="pickup_latitude", dtype=Float32),
        Field(name="dropoff_longitude", dtype=Float32),
        Field(name="dropoff_latitude", dtype=Float32),
        Field(name="payment_type", dtype=Int32),
        Field(name="fare_amount", dtype=Float32),
        Field(name="tip_amount", dtype=Float32),
        Field(name="total_amount", dtype=Float32),
        Field(name="rate_code_id", dtype=Int32),
        Field(name="event_timestamp", dtype=String),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=realtime_trips_stream,
    description="Real-time trip features",
)
def trip_features_stream(df: DataFrame):
    """Process real-time trip data"""
    return df


# Batch feature view for historical trip analysis
historical_trip_features = FeatureView(
    name="historical_trip_features",
    entities=[trip, pickup_location, dropoff_location],
    ttl=timedelta(days=30),
    schema=[
        Field(name="avg_duration", dtype=Float32),
        Field(name="avg_fare", dtype=Float32),
        Field(name="avg_tip_percentage", dtype=Float32),
        Field(name="total_trips", dtype=Int64),
        Field(name="peak_hour_trips", dtype=Int64),
        Field(name="common_payment_type", dtype=Int32),
    ],
    source=historical_trips_source,
    online=True,
    description="Historical trip statistics and patterns",
)

# Batch feature view for location statistics
location_stats_features = FeatureView(
    name="location_stats_features",
    entities=[location],
    ttl=timedelta(days=7),
    schema=[
        Field(name="avg_daily_trips", dtype=Float32),
        Field(name="avg_fare_per_mile", dtype=Float32),
        Field(name="peak_demand_hours", dtype=String),
        Field(name="popular_destinations", dtype=String),
        Field(name="avg_traffic_level", dtype=String),
    ],
    source=location_stats_source,
    online=True,
    description="Location-based statistics and patterns",
)

# Feature view for vendor performance
vendor_performance_features = FeatureView(
    name="vendor_performance_features",
    entities=[vendor],
    ttl=timedelta(days=7),
    schema=[
        Field(name="total_trips", dtype=Int64),
        Field(name="avg_rating", dtype=Float32),
        Field(name="avg_fare_per_trip", dtype=Float32),
        Field(name="avg_tip_percentage", dtype=Float32),
        Field(name="completion_rate", dtype=Float32),
    ],
    source=historical_trips_source,
    online=True,
    description="Vendor performance metrics",
)

# Feature view for payment patterns
payment_pattern_features = FeatureView(
    name="payment_pattern_features",
    entities=[payment_type],
    ttl=timedelta(days=7),
    schema=[
        Field(name="usage_percentage", dtype=Float32),
        Field(name="avg_transaction_amount", dtype=Float32),
        Field(name="peak_usage_hours", dtype=String),
        Field(name="preferred_by_distance", dtype=String),
    ],
    source=historical_trips_source,
    online=True,
    description="Payment method usage patterns",
)
