from typing import Any, Dict

import pandas as pd
from include.common.scripts.monitoring import PipelineMonitoring
from loguru import logger

from airflow.decorators import task

logger = logger.bind(name=__name__)


def categorize_fare(fare: float) -> str:
    """Categorize fare amount into tiers"""
    logger.debug(f"Categorizing fare: {fare}")
    if fare < 10:
        return "very_low"
    elif fare < 20:
        return "low"
    elif fare < 30:
        return "medium"
    elif fare < 50:
        return "high"
    return "very_high"


def categorize_distance(distance: float) -> str:
    """Categorize trip distance into tiers"""
    logger.debug(f"Categorizing distance: {distance}")
    if distance < 1:
        return "very_short"
    elif distance < 3:
        return "short"
    elif distance < 5:
        return "medium"
    elif distance < 10:
        return "long"
    return "very_long"


def calculate_trip_duration(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate trip duration in minutes"""
    logger.debug("Calculating trip durations")
    df["pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df["trip_duration_minutes"] = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60
    return df


def calculate_speed(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate average speed in miles per hour"""
    logger.debug("Calculating average speeds")
    # Avoid division by zero by handling zero duration trips
    df["avg_speed_mph"] = df.apply(
        lambda x: x["trip_distance"] / (x["trip_duration_minutes"] / 60)
        if x["trip_duration_minutes"] > 0
        else 0,
        axis=1
    )
    return df


def transform_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Convert timestamps and add derived time columns"""
    logger.debug("Transforming timestamps")
    
    df["pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    
    # Add time-based columns
    df["pickup_date"] = df["pickup_datetime"].dt.date
    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    df["pickup_day_of_week"] = df["pickup_datetime"].dt.day_name()
    df["pickup_month"] = df["pickup_datetime"].dt.month
    df["is_weekend"] = df["pickup_datetime"].dt.dayofweek.isin([5, 6])
    df["is_rush_hour"] = df["pickup_hour"].isin([7, 8, 9, 16, 17, 18, 19])

    logger.info(f"Transformed timestamps: {len(df)} records processed.")
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns based on taxi trip data"""
    logger.debug("Adding derived columns")
    
    # Add fare and distance tiers
    df["fare_tier"] = df["fare_amount"].apply(categorize_fare)
    df["distance_tier"] = df["trip_distance"].apply(categorize_distance)
    
    # Calculate tip percentage
    df["tip_percentage"] = df.apply(
        lambda x: (x["tip_amount"] / x["fare_amount"]) * 100 if x["fare_amount"] > 0 else 0,
        axis=1
    )
    
    # Flag for shared rides (based on passenger count > 1)
    df["is_shared_ride"] = df["passenger_count"] > 1
    
    # Payment type mapping
    payment_type_map = {
        1: "credit_card",
        2: "cash",
        3: "no_charge",
        4: "dispute"
    }
    df["payment_type_desc"] = df["payment_type"].map(payment_type_map)
    
    # Rate code mapping
    rate_code_map = {
        1: "standard",
        2: "jfk",
        3: "newark",
        4: "nassau_westchester",
        5: "negotiated",
        6: "group_ride"
    }
    df["rate_code_desc"] = df["RateCodeID"].map(rate_code_map)

    logger.info(f"Derived columns added: {len(df)} records processed.")
    return df


def calculate_trip_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate metrics per trip"""
    logger.debug("Calculating trip metrics")
    
    # Calculate trip duration and speed
    df = calculate_trip_duration(df)
    df = calculate_speed(df)
    
    # Calculate total cost components
    df["total_cost_breakdown"] = df.apply(
        lambda x: {
            "base_fare": x["fare_amount"],
            "tips": x["tip_amount"],
            "taxes": x["mta_tax"],
            "extras": x["extra"],
            "tolls": x["tolls_amount"],
            "surcharge": x["improvement_surcharge"]
        },
        axis=1
    )
    
    # Flag potentially problematic trips
    df["is_suspicious"] = (
        (df["trip_duration_minutes"] < 1) |  # Very short duration
        (df["avg_speed_mph"] > 80) |         # Very high speed
        (df["trip_distance"] == 0) |         # Zero distance
        (df["fare_amount"] == 0)             # Zero fare
    )

    logger.info(f"Trip metrics calculated: {len(df)} trips processed.")
    return df


def check_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate records based on record_hash"""
    initial_count = len(df)
    df = df.drop_duplicates(subset=["record_hash"], keep="first")
    duplicate_count = initial_count - len(df)
    logger.info(f"Removed {duplicate_count} duplicate records")
    return df


def prepare_for_serialization(df: pd.DataFrame) -> Dict[str, Any]:
    """Convert DataFrame to serializable format"""
    logger.debug("Preparing DataFrame for serialization")
    # Convert timestamps to ISO format strings
    df["pickup_datetime"] = df["pickup_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["dropoff_datetime"] = df["dropoff_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["pickup_date"] = df["pickup_date"].astype(str)

    # Convert any remaining non-serializable objects
    for col in df.select_dtypes(include=["datetime64"]).columns:
        df[col] = df[col].astype(str)

    logger.info(f"DataFrame prepared for serialization: {len(df)} records.")
    return df.to_dict(orient="records")


@task()
def transform_data(validated_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform the validated taxi trip data"""
    logger.info("Starting data transformation")
    try:
        # Convert to DataFrame and log initial state
        df = pd.DataFrame(validated_data["data"])

        if len(df) == 0:
            logger.warning("No data to transform")
            return {
                "data": [],
                "success": False,
                "message": "No data to transform",
                "skip_downstream": True,
            }

        logger.debug(f"Initial DataFrame columns: {df.columns.tolist()}")
        logger.debug(f"DataFrame shape: {df.shape}")

        # Check and remove duplicates
        df = check_duplicates(df)

        # Apply transformations
        try:
            df = transform_timestamps(df)
            df = add_derived_columns(df)
            df = calculate_trip_metrics(df)
        except Exception as e:
            logger.error(f"Failed during transformation: {str(e)}")
            raise

        # Calculate transformation metrics
        metrics = {
            "total_trips": len(df),
            "total_passengers": df["passenger_count"].sum(),
            "total_distance": df["trip_distance"].sum(),
            "total_revenue": df["total_amount"].sum(),
            "avg_trip_duration": df["trip_duration_minutes"].mean(),
            "avg_speed": df["avg_speed_mph"].mean(),
            "avg_fare": df["fare_amount"].mean(),
            "avg_tip_percentage": df["tip_percentage"].mean(),
            "payment_type_distribution": df["payment_type_desc"].value_counts().to_dict(),
            "fare_tier_distribution": df["fare_tier"].value_counts().to_dict(),
            "distance_tier_distribution": df["distance_tier"].value_counts().to_dict(),
            "suspicious_trips": df["is_suspicious"].sum(),
            "shared_rides_percentage": (df["is_shared_ride"].sum() / len(df)) * 100
        }

        logger.info(f"Transformation metrics calculated: {metrics}")

        # Log metrics
        PipelineMonitoring.log_metrics(metrics)

        # Convert to serializable format
        serializable_data = prepare_for_serialization(df)

        logger.info("Data transformation completed successfully")
        return {"data": serializable_data, "metrics": metrics}

    except Exception as e:
        logger.error(f"Failed to transform data: {str(e)}", exc_info=True)
        raise Exception(f"Failed to transform data: {str(e)}")
