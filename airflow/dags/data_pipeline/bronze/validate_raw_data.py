from datetime import datetime
from hashlib import sha256
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import pendulum
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from include.common.scripts.monitoring import PipelineMonitoring
from loguru import logger

from airflow.decorators import task

logger = logger.bind(name=__name__)


def extract_payload(record: Dict[str, Any]) -> Dict[str, Any]:
    """Extract payload from nested record structure"""
    try:
        if isinstance(record.get("payload"), dict):
            return record["payload"]
        return record
    except Exception:
        return record


def validate_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validate a single record against business rules for NYC taxi data"""
    try:
        # Extract payload if nested
        data = extract_payload(record)

        # Check required fields
        required_fields = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount"
        ]

        for field in required_fields:
            if field not in data:
                logger.error(f"Missing required field {field} for record: {data}")
                return False, f"Missing required field: {field}"

        # Validate pickup and dropoff times
        try:
            pickup_time = pd.to_datetime(data["tpep_pickup_datetime"])
            dropoff_time = pd.to_datetime(data["tpep_dropoff_datetime"])
            
            if dropoff_time < pickup_time:
                return False, "Dropoff time cannot be earlier than pickup time"
            
            # Check if times are not in the future
            current_time = pd.Timestamp.now()
            if pickup_time > current_time or dropoff_time > current_time:
                return False, "Trip times cannot be in the future"
        except Exception as e:
            return False, f"Invalid datetime format: {str(e)}"

        # Validate numeric fields
        if data["passenger_count"] <= 0:
            return False, "Passenger count must be positive"

        if data["trip_distance"] < 0:
            return False, "Trip distance cannot be negative"

        # Validate fare amounts
        monetary_fields = [
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount"
        ]
        
        for field in monetary_fields:
            if field in data and data[field] is not None:
                if not isinstance(data[field], (int, float)) or data[field] < 0:
                    return False, f"{field} must be a non-negative number"

        # Validate coordinates if present
        if all(field in data for field in ["pickup_latitude", "pickup_longitude"]):
            if not (-90 <= data["pickup_latitude"] <= 90):
                return False, "Invalid pickup latitude"
            if not (-180 <= data["pickup_longitude"] <= 180):
                return False, "Invalid pickup longitude"

        if all(field in data for field in ["dropoff_latitude", "dropoff_longitude"]):
            if not (-90 <= data["dropoff_latitude"] <= 90):
                return False, "Invalid dropoff latitude"
            if not (-180 <= data["dropoff_longitude"] <= 180):
                return False, "Invalid dropoff longitude"

        # Validate payment type if present
        if "payment_type" in data and data["payment_type"] is not None:
            valid_payment_types = [1, 2, 3, 4]  # Common payment types in NYC taxi data
            if data["payment_type"] not in valid_payment_types:
                return False, f"Invalid payment type. Must be one of {valid_payment_types}"

        # Validate rate code if present
        if "RateCodeID" in data and data["RateCodeID"] is not None:
            valid_rate_codes = [1, 2, 3, 4, 5, 6]  # Standard rate codes
            if data["RateCodeID"] not in valid_rate_codes:
                return False, f"Invalid rate code. Must be one of {valid_rate_codes}"

        return True, None
    except Exception as e:
        logger.exception("Error validating record: {}", record)
        return False, str(e)


def generate_record_hash(record: Dict[str, Any]) -> str:
    """Generate a unique hash for a taxi trip record based on business keys"""
    try:
        data = extract_payload(record)
        # Using combination of pickup time, dropoff time, and coordinates as unique identifier
        key_fields = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "pickup_longitude",
            "pickup_latitude",
            "dropoff_longitude",
            "dropoff_latitude",
            "VendorID"
        ]
        key_string = "|".join(str(data.get(field, "")) for field in key_fields)
        return sha256(key_string.encode()).hexdigest()
    except Exception as e:
        logger.error(f"Error generating hash for record: {str(e)}")
        return sha256(str(record).encode()).hexdigest()


def enrich_record(record: Dict[str, Any], record_hash: str) -> Dict[str, Any]:
    """Add metadata to a taxi trip record"""
    try:
        # Keep original record structure
        enriched = record.copy()
        enriched["processed_date"] = datetime.now(
            tz=pendulum.timezone("UTC")
        ).isoformat()
        enriched["processing_pipeline"] = "nyc_taxi_etl"
        enriched["valid"] = "TRUE"
        enriched["record_hash"] = record_hash

        # If payload exists, also add metadata there
        if isinstance(enriched.get("payload"), dict):
            enriched["payload"]["processed_date"] = enriched["processed_date"]
            enriched["payload"]["processing_pipeline"] = enriched["processing_pipeline"]
            enriched["payload"]["valid"] = enriched["valid"]
            enriched["payload"]["record_hash"] = enriched["record_hash"]

        return enriched
    except Exception as e:
        logger.error(f"Error enriching record: {str(e)}")
        return record


@task(task_id="quality_check_raw_data")
def validate_raw_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate NYC taxi trip data using both Great Expectations and custom validation"""
    try:
        # Convert input data to DataFrame
        df = pd.DataFrame(raw_data["data"])
        logger.info(f"Initial columns: {df.columns.tolist()}")

        # Keep one sample record for schema
        sample_record = df.iloc[0].to_dict() if not df.empty else None

        # Add record hash
        df["record_hash"] = df.apply(
            lambda x: generate_record_hash(x.to_dict()), axis=1
        )

        # Custom validation
        records = df.to_dict("records")
        validation_results = [validate_record(record) for record in records]
        valid_records = [
            record
            for record, (is_valid, _) in zip(records, validation_results)
            if is_valid
        ]
        validation_errors = {
            i: error
            for i, (is_valid, error) in enumerate(validation_results)
            if not is_valid
        }

        # If no valid records but we have a sample, create a dummy record
        if not valid_records and sample_record:
            logger.warning("No valid records, using sample record for schema")
            dummy_record = sample_record.copy()
            # Mark it as dummy record
            dummy_record["is_dummy"] = True
            dummy_record["record_hash"] = generate_record_hash(dummy_record)
            valid_records = [dummy_record]

        # Convert valid records back to DataFrame
        valid_df = pd.DataFrame(valid_records)

        # Validate using Great Expectations
        gx_validate = GreatExpectationsOperator(
            task_id="quality_check_raw_data",
            data_context_root_dir="include/gx",
            dataframe_to_validate=valid_df,
            data_asset_name="nyc_taxi_data_asset",
            execution_engine="PandasExecutionEngine",
            expectation_suite_name="nyc_taxi_suite",
            return_json_dict=True,
        )

        validation_result = gx_validate.execute(context={})

        # Calculate metrics
        metrics = {
            "total_records": len(df),
            "valid_records": len(valid_records),
            "invalid_records": len(df) - len(valid_records),
            "validation_errors": validation_errors,
            "contains_dummy": bool(not valid_records and sample_record),
        }

        # Log metrics
        PipelineMonitoring.log_metrics(metrics)

        # Enrich and flatten valid records
        enriched_data = []
        for record in valid_records:
            enriched = enrich_record(record, record["record_hash"])
            enriched_data.append(enriched)

        return {"data": enriched_data, "metrics": metrics}

    except Exception as e:
        logger.exception("Failed to validate data")
        raise Exception(f"Failed to validate data: {str(e)}")
