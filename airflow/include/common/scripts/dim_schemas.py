from typing import Dict, List


class DimVendorSchema:
    """Schema for vendor dimension table"""

    table_schema: Dict[str, str] = {
        "vendor_id": "INTEGER PRIMARY KEY",
        "vendor_name": "VARCHAR(100)",
        "active_vehicles": "INTEGER",
        "rating": "DECIMAL(3,2)",
    }
    indexes: List[str] = [
        "CREATE INDEX idx_dim_vendor_id ON dim_vendor (vendor_id)",
    ]


class DimLocationSchema:
    """Schema for location dimension table"""

    table_schema: Dict[str, str] = {
        "location_id": "BIGINT PRIMARY KEY",
        "latitude": "DECIMAL(9,6)",
        "longitude": "DECIMAL(9,6)",
        "zone": "VARCHAR(100)",
        "borough": "VARCHAR(50)",
        "service_zone": "VARCHAR(50)",
    }
    indexes: List[str] = [
        "CREATE INDEX idx_dim_location_id ON dim_location (location_id)",
        "CREATE INDEX idx_dim_location_coords ON dim_location (latitude, longitude)",
        "CREATE INDEX idx_dim_location_zone ON dim_location (zone)",
    ]


class DimPaymentSchema:
    """Schema for payment type dimension table"""

    table_schema: Dict[str, str] = {
        "payment_type_id": "INTEGER PRIMARY KEY",
        "payment_type_desc": "VARCHAR(50)",
        "payment_category": "VARCHAR(50)",
    }
    indexes: List[str] = [
        "CREATE INDEX idx_dim_payment_type ON dim_payment (payment_type_id)",
    ]


class DimRateCodeSchema:
    """Schema for rate code dimension table"""

    table_schema: Dict[str, str] = {
        "rate_code_id": "INTEGER PRIMARY KEY",
        "rate_code_desc": "VARCHAR(50)",
        "base_fare": "DECIMAL(10,2)",
        "surcharge": "DECIMAL(10,2)",
    }
    indexes: List[str] = [
        "CREATE INDEX idx_dim_rate_code ON dim_rate_code (rate_code_id)",
    ]


class DimDateSchema:
    """Schema for date dimension table"""

    table_schema: Dict[str, str] = {
        "pickup_date": "DATE PRIMARY KEY",
        "pickup_hour": "INTEGER",
        "pickup_day_of_week": "VARCHAR(50)",
        "pickup_month": "INTEGER",
        "is_weekend": "BOOLEAN",
        "is_holiday": "BOOLEAN",
        "is_rush_hour": "BOOLEAN",
        "day_period": "VARCHAR(20)",
    }
    indexes: List[str] = [
        "CREATE INDEX idx_dim_date_date ON dim_date (pickup_date)",
        "CREATE INDEX idx_dim_date_hour ON dim_date (pickup_hour)",
        "CREATE INDEX idx_dim_date_period ON dim_date (day_period)",
    ]
