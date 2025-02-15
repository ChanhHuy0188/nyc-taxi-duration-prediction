import os
from time import sleep

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from loguru import logger
from models import TaxiTrip, Base
from postgresql_client import PostgresSQLClient
from sqlalchemy import create_engine, inspect
from sqlalchemy_utils import database_exists, create_database

load_dotenv()

SAMPLE_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "data", "green_tripdata_2024-02.parquet"
)
RECORDS_PER_SECOND = 1  # Set the rate limit to 1 record per second


def format_record(row):
    try:
        # Handle pickup datetime
        pickup_datetime = None
        if pd.notnull(row.get("lpep_pickup_datetime")):
            pickup_datetime = pd.to_datetime(row["lpep_pickup_datetime"])
        elif pd.notnull(row.get("tpep_pickup_datetime")):
            pickup_datetime = pd.to_datetime(row["tpep_pickup_datetime"])
        else:
            pickup_datetime = pd.Timestamp.now()

        # Handle dropoff datetime
        dropoff_datetime = None
        if pd.notnull(row.get("lpep_dropoff_datetime")):
            dropoff_datetime = pd.to_datetime(row["lpep_dropoff_datetime"])
        elif pd.notnull(row.get("tpep_dropoff_datetime")):
            dropoff_datetime = pd.to_datetime(row["tpep_dropoff_datetime"])
        else:
            dropoff_datetime = pd.Timestamp.now()

        # Safe conversion functions
        def safe_int(value, default=0):
            try:
                if pd.isna(value):
                    return default
                return int(value)
            except (ValueError, TypeError):
                return default

        def safe_float(value, default=0.0):
            try:
                if pd.isna(value):
                    return default
                return float(value)
            except (ValueError, TypeError):
                return default

        def safe_string(value, default="N"):
            if pd.isna(value):
                return default
            return str(value)

        return TaxiTrip(
            VendorID=safe_int(row.get("VendorID")),
            tpep_pickup_datetime=pickup_datetime,
            tpep_dropoff_datetime=dropoff_datetime,
            passenger_count=safe_int(row.get("passenger_count")),
            trip_distance=safe_float(row.get("trip_distance")),
            pickup_longitude=safe_float(row.get("pickup_longitude")),
            pickup_latitude=safe_float(row.get("pickup_latitude")),
            RateCodeID=safe_int(row.get("RateCodeID")),
            store_and_fwd_flag=safe_string(row.get("store_and_fwd_flag")),
            dropoff_longitude=safe_float(row.get("dropoff_longitude")),
            dropoff_latitude=safe_float(row.get("dropoff_latitude")),
            payment_type=safe_int(row.get("payment_type")),
            fare_amount=safe_float(row.get("fare_amount")),
            extra=safe_float(row.get("extra")),
            mta_tax=safe_float(row.get("mta_tax")),
            tip_amount=safe_float(row.get("tip_amount")),
            tolls_amount=safe_float(row.get("tolls_amount")),
            improvement_surcharge=safe_float(row.get("improvement_surcharge")),
            total_amount=safe_float(row.get("total_amount"))
        )
    except Exception as e:
        logger.warning(f"Error formatting record: {str(e)}, row: {row}")
        # Return record with default values if there's an error
        return TaxiTrip(
            VendorID=0,
            tpep_pickup_datetime=pd.Timestamp.now(),
            tpep_dropoff_datetime=pd.Timestamp.now(),
            passenger_count=0,
            trip_distance=0.0,
            pickup_longitude=0.0,
            pickup_latitude=0.0,
            RateCodeID=0,
            store_and_fwd_flag="N",
            dropoff_longitude=0.0,
            dropoff_latitude=0.0,
            payment_type=0,
            fare_amount=0.0,
            extra=0.0,
            mta_tax=0.0,
            tip_amount=0.0,
            tolls_amount=0.0,
            improvement_surcharge=0.0,
            total_amount=0.0
        )


def load_sample_data():
    """Load and prepare sample data from parquet file"""
    try:
        logger.info(f"Loading sample data from {SAMPLE_DATA_PATH}")
        # Read parquet file with handling for missing values
        df = pd.read_parquet(
            SAMPLE_DATA_PATH, 
            engine="fastparquet"
        )
        
        # Replace infinite values with NaN
        df = df.replace([np.inf, -np.inf], np.nan)
        
        records = []
        for idx, row in df.iterrows():
            record = format_record(row)
            if record is not None:
                records.append(record)
            
            # Log progress for large datasets
            if idx > 0 and idx % 10000 == 0:
                logger.info(f"Processed {idx} records...")
                
        logger.success(
            f"Successfully loaded {len(records)} records from {SAMPLE_DATA_PATH}"
        )
        return records
    except Exception as e:
        logger.error(f"Error loading sample data: {str(e)}")
        raise


def setup_database():
    """Check if database exists and create it if it doesn't"""
    try:
        # Create database URL
        db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        
        # Check if database exists
        if not database_exists(db_url):
            logger.info(f"Database {os.getenv('POSTGRES_DB')} does not exist. Creating...")
            create_database(db_url)
            logger.success(f"Database {os.getenv('POSTGRES_DB')} created successfully!")
        else:
            logger.info(f"Database {os.getenv('POSTGRES_DB')} already exists")

        # Create engine and check/create tables
        engine = create_engine(db_url)
        inspector = inspect(engine)
        
        # Check if our table exists
        if not inspector.has_table('taxi_trips'):
            logger.info("Table 'taxi_trips' does not exist. Creating tables...")
            Base.metadata.create_all(engine)
            logger.success("Tables created successfully!")
        else:
            logger.info("Table 'taxi_trips' already exists")
            
        return engine
    except Exception as e:
        logger.error(f"Error setting up database: {str(e)}")
        raise


def main():
    logger.info("Starting data insertion process")
    
    # Setup database and tables
    try:
        engine = setup_database()
        logger.info("Database setup completed")
    except Exception as e:
        logger.error(f"Failed to setup database: {str(e)}")
        return

    # Initialize PostgreSQL client
    pc = PostgresSQLClient(
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    logger.info("Successfully connected to PostgreSQL database")

    # Load and process records
    records = load_sample_data()
    valid_records = 0
    invalid_records = 0

    # Get session
    session = pc.get_session()

    logger.info("Starting record insertion")
    try:
        for record in records:
            try:
                # Insert single record
                session.add(record)
                session.commit()
                valid_records += 1
                
                if valid_records % 10 == 0:
                    logger.info(f"Processed {valid_records} valid records")
                
                # Sleep to maintain rate limit
                sleep(1/RECORDS_PER_SECOND)
                
            except Exception as e:
                logger.error(f"Failed to insert record: {str(e)}")
                invalid_records += 1
                session.rollback()

    except Exception as e:
        logger.error(f"Insertion error: {str(e)}")
        session.rollback()
    finally:
        session.close()

    logger.info("\nFinal Summary:")
    logger.info(f"Total records processed: {len(records)}")
    logger.success(f"Valid records inserted: {valid_records}")
    logger.warning(f"Invalid records skipped: {invalid_records}")


if __name__ == "__main__":
    main()
