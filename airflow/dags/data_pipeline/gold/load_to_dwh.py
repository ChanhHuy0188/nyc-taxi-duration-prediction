from typing import Any, Dict

import pandas as pd
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.common.scripts.db_utils import batch_insert_data, create_schema_and_table
from include.common.scripts.dim_schemas import (
    DimDateSchema,
    DimLocationSchema,
    DimPaymentSchema,
    DimRateCodeSchema,
    DimVendorSchema,
)
from include.common.scripts.fact_schemas import FactTripSchema
from include.common.scripts.sql_utils import load_sql_template
from loguru import logger

logger = logger.bind(name=__name__)


def create_dim_vendor(df: pd.DataFrame) -> pd.DataFrame:
    """Create vendor dimension table"""
    logger.info("Creating vendor dimension table")
    dim_vendor = df[["VendorID"]].copy()
    dim_vendor = dim_vendor.drop_duplicates()
    dim_vendor.columns = ["vendor_id"]
    return dim_vendor


def create_dim_payment(df: pd.DataFrame) -> pd.DataFrame:
    """Create payment type dimension table"""
    logger.info("Creating payment type dimension table")
    dim_payment = df[["payment_type", "payment_type_desc"]].copy()
    dim_payment = dim_payment.drop_duplicates()
    dim_payment.columns = ["payment_type_id", "payment_type_desc"]
    return dim_payment


def create_dim_rate_code(df: pd.DataFrame) -> pd.DataFrame:
    """Create rate code dimension table"""
    logger.info("Creating rate code dimension table")
    dim_rate = df[["RateCodeID", "rate_code_desc"]].copy()
    dim_rate = dim_rate.drop_duplicates()
    dim_rate.columns = ["rate_code_id", "rate_code_desc"]
    return dim_rate


def create_dim_location(df: pd.DataFrame) -> pd.DataFrame:
    """Create location dimension table"""
    logger.info("Creating location dimension table")
    # Create pickup locations
    pickup_locations = df[[
        "pickup_longitude", 
        "pickup_latitude"
    ]].drop_duplicates()
    pickup_locations["location_id"] = range(1, len(pickup_locations) + 1)
    pickup_locations.columns = ["longitude", "latitude", "location_id"]
    
    # Create dropoff locations
    dropoff_locations = df[[
        "dropoff_longitude", 
        "dropoff_latitude"
    ]].drop_duplicates()
    # Filter out locations already in pickup_locations
    dropoff_locations = dropoff_locations[~dropoff_locations.apply(tuple, 1).isin(
        pickup_locations[["longitude", "latitude"]].apply(tuple, 1)
    )]
    if not dropoff_locations.empty:
        max_id = pickup_locations["location_id"].max()
        dropoff_locations["location_id"] = range(max_id + 1, max_id + 1 + len(dropoff_locations))
        dropoff_locations.columns = ["longitude", "latitude", "location_id"]
        
        # Combine pickup and dropoff locations
        dim_location = pd.concat([pickup_locations, dropoff_locations])
    else:
        dim_location = pickup_locations
    
    return dim_location


def create_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Create date dimension table"""
    logger.info("Creating date dimension table")
    dim_date = df[[
        "pickup_date",
        "pickup_hour",
        "pickup_day_of_week",
        "pickup_month",
        "is_weekend",
        "is_rush_hour"
    ]].copy()
    dim_date = dim_date.drop_duplicates()
    return dim_date


def create_fact_trips(df: pd.DataFrame, dims: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create fact table for taxi trips"""
    logger.info("Creating fact trips table")
    
    # Get location IDs for pickup and dropoff
    location_dim = dims["dwh.dim_location"]
    
    # Map pickup locations
    pickup_mapping = location_dim.set_index(
        ["longitude", "latitude"]
    )["location_id"].to_dict()
    df["pickup_location_id"] = df.apply(
        lambda x: pickup_mapping.get((x["pickup_longitude"], x["pickup_latitude"])),
        axis=1
    )
    
    # Map dropoff locations
    dropoff_mapping = location_dim.set_index(
        ["longitude", "latitude"]
    )["location_id"].to_dict()
    df["dropoff_location_id"] = df.apply(
        lambda x: dropoff_mapping.get((x["dropoff_longitude"], x["dropoff_latitude"])),
        axis=1
    )
    
    fact_trips = df[[
        "VendorID",
        "pickup_date",
        "pickup_location_id",
        "dropoff_location_id",
        "passenger_count",
        "trip_distance",
        "payment_type",
        "RateCodeID",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "trip_duration_minutes",
        "avg_speed_mph",
        "is_shared_ride",
        "is_suspicious"
    ]].copy()
    
    # Rename columns to match schema
    fact_trips.columns = [
        "vendor_id",
        "trip_date",
        "pickup_location_id",
        "dropoff_location_id",
        "passenger_count",
        "trip_distance",
        "payment_type_id",
        "rate_code_id",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "trip_duration_minutes",
        "avg_speed_mph",
        "is_shared_ride",
        "is_suspicious"
    ]
    
    return fact_trips


@task()
def load_dimensions_and_facts(transformed_data: Dict[str, Any]) -> Dict[str, Any]:
    """Load dimensional model into Data Warehouse"""
    logger.info("Loading dimensions and facts into Data Warehouse")
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")
        df = pd.DataFrame(transformed_data["data"])

        # Create schema if not exists
        postgres_hook.run("CREATE SCHEMA IF NOT EXISTS dwh;")

        # Create dimensions
        dims = {
            "dwh.dim_vendor": create_dim_vendor(df),
            "dwh.dim_payment": create_dim_payment(df),
            "dwh.dim_rate_code": create_dim_rate_code(df),
            "dwh.dim_location": create_dim_location(df),
            "dwh.dim_date": create_dim_date(df),
        }

        # Map table names to schema classes
        schema_mapping = {
            "dwh.dim_vendor": DimVendorSchema,
            "dwh.dim_payment": DimPaymentSchema,
            "dwh.dim_rate_code": DimRateCodeSchema,
            "dwh.dim_location": DimLocationSchema,
            "dwh.dim_date": DimDateSchema,
        }

        # Create fact table
        fact_trips = create_fact_trips(df, dims)

        # Create and load dimension tables
        for table_name, dim_df in dims.items():
            schema_class = schema_mapping[table_name]
            create_schema_and_table(postgres_hook, schema_class, table_name)
            batch_insert_data(postgres_hook, dim_df, table_name)

        # Create and load fact table
        create_schema_and_table(postgres_hook, FactTripSchema, "dwh.fact_trips")
        batch_insert_data(postgres_hook, fact_trips, "dwh.fact_trips")

        # Create useful views
        create_analytical_views(postgres_hook)

        return {
            "data": df,
            "success": True,
        }

    except Exception as e:
        logger.error(f"Failed to load dimensional model: {str(e)}")
        raise Exception(f"Failed to load dimensional model: {str(e)}")


def create_analytical_views(postgres_hook: PostgresHook) -> None:
    """Create useful views for analysis"""
    logger.info("Creating analytical views")
    try:
        # Load and execute SQL file
        sql = load_sql_template("views/taxi_analytical_views.sql")
        postgres_hook.run(sql)

        # Verify views exist
        views = [
            "vw_daily_revenue_summary",
            "vw_location_performance",
            "vw_vendor_performance",
            "vw_payment_patterns",
            "vw_trip_statistics"
        ]
        for view_name in views:
            verification_sql = f"""
            SELECT EXISTS (
                SELECT FROM pg_views
                WHERE schemaname = 'dwh'
                AND viewname = '{view_name}'
            );
            """
            exists = postgres_hook.get_first(verification_sql)[0]
            if exists:
                logger.info(f"Successfully verified view exists: {view_name}")
            else:
                logger.error(f"View was not created: {view_name}")

    except Exception as e:
        logger.error(f"Failed to create analytical views: {str(e)}")
        raise Exception(f"Failed to create analytical views: {str(e)}")
