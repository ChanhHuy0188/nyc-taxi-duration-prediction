import os
from datetime import timedelta

from feast import KafkaSource
from feast.data_format import JsonFormat
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)


def read_sql_file(filename):
    """Read SQL query from file."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(current_dir, "sql", filename)
    with open(sql_path, "r") as f:
        return f.read().strip()


# Batch source for historical taxi trips
historical_trips_source = PostgreSQLSource(
    name="historical_trips",
    query=read_sql_file("historical_trips.sql"),
    timestamp_field="pickup_datetime",
)

# Batch source for location statistics
location_stats_source = PostgreSQLSource(
    name="location_stats",
    query=read_sql_file("location_stats.sql"),
    timestamp_field="timestamp",
)

# Stream source for real-time taxi trips
realtime_trips_stream = KafkaSource(
    name="realtime_trips_stream",
    kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    topic=os.getenv("KAFKA_TRIPS_TOPIC", "nyc.taxi.trips.validated"),
    timestamp_field="event_timestamp",
    batch_source=historical_trips_source,
    message_format=JsonFormat(
        schema_json="""
        {
            "type": "record",
            "name": "taxi_trip",
            "fields": [
                {"name": "trip_id", "type": "string"},
                {"name": "vendor_id", "type": "int"},
                {"name": "pickup_datetime", "type": "string"},
                {"name": "dropoff_datetime", "type": "string"},
                {"name": "passenger_count", "type": "int"},
                {"name": "trip_distance", "type": "double"},
                {"name": "pickup_longitude", "type": "double"},
                {"name": "pickup_latitude", "type": "double"},
                {"name": "dropoff_longitude", "type": "double"},
                {"name": "dropoff_latitude", "type": "double"},
                {"name": "payment_type", "type": "int"},
                {"name": "fare_amount", "type": "double"},
                {"name": "tip_amount", "type": "double"},
                {"name": "total_amount", "type": "double"},
                {"name": "rate_code_id", "type": "int"},
                {"name": "event_timestamp", "type": "string"}
            ]
        }
        """
    ),
    description="Stream of real-time taxi trips from Kafka",
    tags={"team": "ml_team", "product": "trip_prediction"},
    watermark_delay_threshold=timedelta(minutes=5),
)

# Stream source for real-time location updates
location_updates_stream = KafkaSource(
    name="location_updates_stream",
    kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    topic=os.getenv("KAFKA_LOCATIONS_TOPIC", "nyc.taxi.locations"),
    timestamp_field="event_timestamp",
    batch_source=location_stats_source,
    message_format=JsonFormat(
        schema_json="""
        {
            "type": "record",
            "name": "location_update",
            "fields": [
                {"name": "location_id", "type": "int"},
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"},
                {"name": "current_demand", "type": "int"},
                {"name": "available_taxis", "type": "int"},
                {"name": "surge_multiplier", "type": "double"},
                {"name": "weather_condition", "type": "string"},
                {"name": "traffic_level", "type": "string"},
                {"name": "event_timestamp", "type": "string"}
            ]
        }
        """
    ),
    description="Stream of real-time location updates from Kafka",
    tags={"team": "ml_team", "product": "demand_prediction"},
    watermark_delay_threshold=timedelta(minutes=1),
)
