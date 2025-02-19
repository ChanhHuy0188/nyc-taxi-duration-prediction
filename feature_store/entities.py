# entities.py
from feast import Entity, ValueType

# Trip entity
trip = Entity(
    name="trip_id",
    value_type=ValueType.STRING,
    description="Unique taxi trip identifier",
    join_keys=["trip_id"],
)

# Location entity - Changed to use location_id as single join key
location = Entity(
    name="location_id",  # Changed from "location"
    value_type=ValueType.INT64,  # Changed to INT64 to match your schema
    description="Geographic location identifier",
    join_keys=["location_id"],  # Single join key instead of [latitude, longitude]
)

# Vendor entity
vendor = Entity(
    name="vendor_id",
    value_type=ValueType.INT32,
    description="Taxi vendor identifier",
    join_keys=["vendor_id"],
)

# Payment type entity
payment_type = Entity(
    name="payment_type",
    value_type=ValueType.INT32,
    description="Payment method identifier",
    join_keys=["payment_type_id"],
)

# Rate code entity
rate_code = Entity(
    name="rate_code",
    value_type=ValueType.INT32,
    description="Taxi rate code identifier",
    join_keys=["rate_code_id"],
)

# Pickup location entity
pickup_location = Entity(
    name="pickup_location",
    value_type=ValueType.INT64,
    description="Pickup location identifier",
    join_keys=["pickup_location_id"],
)

# Dropoff location entity
dropoff_location = Entity(
    name="dropoff_location",
    value_type=ValueType.INT64,
    description="Dropoff location identifier",
    join_keys=["dropoff_location_id"],
)
