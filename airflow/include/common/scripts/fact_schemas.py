from typing import Dict, List


class FactTripSchema:
    """Schema for fact taxi trips table"""

    table_schema: Dict[str, str] = {
        "trip_id": "BIGSERIAL PRIMARY KEY",
        "vendor_id": "INTEGER REFERENCES dim_vendor(vendor_id)",
        "pickup_location_id": "BIGINT REFERENCES dim_location(location_id)",
        "dropoff_location_id": "BIGINT REFERENCES dim_location(location_id)",
        "pickup_date": "DATE REFERENCES dim_date(pickup_date)",
        "pickup_datetime": "TIMESTAMP",
        "dropoff_datetime": "TIMESTAMP",
        "passenger_count": "INTEGER",
        "trip_distance": "DECIMAL(10,2)",
        "payment_type_id": "INTEGER REFERENCES dim_payment(payment_type_id)",
        "rate_code_id": "INTEGER REFERENCES dim_rate_code(rate_code_id)",
        "fare_amount": "DECIMAL(10,2)",
        "extra": "DECIMAL(10,2)",
        "mta_tax": "DECIMAL(10,2)",
        "tip_amount": "DECIMAL(10,2)",
        "tolls_amount": "DECIMAL(10,2)",
        "improvement_surcharge": "DECIMAL(10,2)",
        "total_amount": "DECIMAL(10,2)",
        "trip_duration_minutes": "DECIMAL(10,2)",
        "avg_speed_mph": "DECIMAL(10,2)",
        "is_shared_ride": "BOOLEAN",
        "is_rush_hour": "BOOLEAN",
        "is_suspicious": "BOOLEAN",
    }
    indexes: List[str] = [
        "CREATE INDEX idx_fact_trips_vendor ON fact_trips (vendor_id)",
        "CREATE INDEX idx_fact_trips_pickup_loc ON fact_trips (pickup_location_id)",
        "CREATE INDEX idx_fact_trips_dropoff_loc ON fact_trips (dropoff_location_id)",
        "CREATE INDEX idx_fact_trips_pickup_date ON fact_trips (pickup_date)",
        "CREATE INDEX idx_fact_trips_payment ON fact_trips (payment_type_id)",
        "CREATE INDEX idx_fact_trips_rate_code ON fact_trips (rate_code_id)",
        "CREATE INDEX idx_fact_trips_datetime ON fact_trips (pickup_datetime, dropoff_datetime)",
        "CREATE INDEX idx_fact_trips_distance ON fact_trips (trip_distance)",
        "CREATE INDEX idx_fact_trips_duration ON fact_trips (trip_duration_minutes)",
        "CREATE INDEX idx_fact_trips_amount ON fact_trips (total_amount)",
    ]
