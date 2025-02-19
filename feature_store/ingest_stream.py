import os
import sys
import threading
import time
import warnings
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import pyspark.sql.functions as F
from feast import FeatureStore
from feast.data_source import PushMode
from feast.infra.contrib.spark_kafka_processor import (
    SparkKafkaProcessor,
    SparkProcessorConfig,
)
from loguru import logger
from offline_write_batch import PostgreSQLOfflineWriter
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Ignore all warnings
warnings.filterwarnings("ignore")

# Set up logging
logger.remove()
logger.add(sys.stdout, level="DEBUG")
logger.add("logs/ingest_stream.log", level="DEBUG")

# Kafka environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TRIPS_TOPIC = os.getenv("KAFKA_TRIPS_TOPIC", "tracking.data.validated")
KAFKA_LOCATIONS_TOPIC = os.getenv("KAFKA_LOCATIONS_TOPIC", "tracking.data.validated")

# PostgreSQL environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "dwh")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "feature_store")
POSTGRES_USER = os.getenv("POSTGRES_USER", "dwh")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "dwh")

# Schema for taxi trip data
TRIP_SCHEMA = StructType([
    StructField("trip_id", StringType(), True),
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("rate_code_id", IntegerType(), True),
    StructField("event_timestamp", TimestampType(), True),
])

# Schema for location updates
LOCATION_SCHEMA = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("current_demand", IntegerType(), True),
    StructField("available_taxis", IntegerType(), True),
    StructField("surge_multiplier", DoubleType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("traffic_level", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
])

# Set Spark packages
SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.spark:spark-avro_2.12:3.5.0,"
    "org.apache.kafka:kafka-clients:3.4.0"
)

# Create SparkSession
spark = (
    SparkSession.builder.master("local[*]")
    .appName("nyc-taxi-feature-ingestion")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.shuffle.partitions", 8)
    .config("spark.streaming.kafka.maxRatePerPartition", 100)
    .config("spark.streaming.backpressure.enabled", True)
    .config("spark.kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .config("spark.kafka.failOnDataLoss", "false")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.streaming.checkpointLocation", "checkpoints")
    .getOrCreate()
)

# Initialize PostgreSQL writer
postgres_writer = PostgreSQLOfflineWriter(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DB,
    db_schema=POSTGRES_SCHEMA,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
)

# Initialize the FeatureStore
store = FeatureStore(repo_path=".")

# Session data tracking (optional usage)
session_activity_counts = defaultdict(int)
session_last_seen = defaultdict(datetime.now)
SESSION_TIMEOUT = timedelta(hours=24)


def initialize_feature_tables():
    """Initialize feature tables in PostgreSQL."""
    try:
        # Trip features schema
        trip_feature_names = [
            "trip_distance", "fare_amount", "tip_amount", "total_amount",
            "passenger_count", "trip_duration_minutes", "avg_speed_mph",
            "is_shared_ride", "is_suspicious"
        ]
        trip_feature_types = [
            "float64", "float64", "float64", "float64",
            "int32", "float64", "float64",
            "boolean", "boolean"
        ]
        trip_entity_names = [
            "trip_id", "vendor_id", "pickup_location_id", "dropoff_location_id",
            "payment_type", "rate_code_id", "event_timestamp"
        ]
        trip_entity_types = [
            "string", "int32", "int64", "int64",
            "int32", "int32", "datetime64[ns]"
        ]

        # Location features schema
        location_feature_names = [
            "current_demand", "available_taxis", "surge_multiplier",
            "weather_condition", "traffic_level"
        ]
        location_feature_types = [
            "int32", "int32", "float64",
            "string", "string"
        ]
        location_entity_names = [
            "location_id", "latitude", "longitude", "event_timestamp"
        ]
        location_entity_types = [
            "int64", "float64", "float64", "datetime64[ns]"
        ]

        # Create tables
        postgres_writer.create_table_from_features(
            table_name="trip_features",
            feature_names=trip_feature_names,
            feature_types=trip_feature_types,
            entity_names=trip_entity_names,
            entity_types=trip_entity_types,
        )

        postgres_writer.create_table_from_features(
            table_name="location_features",
            feature_names=location_feature_names,
            feature_types=location_feature_types,
            entity_names=location_entity_names,
            entity_types=location_entity_types,
        )

        logger.info("Successfully initialized feature tables in PostgreSQL")

    except Exception as e:
        logger.error(f"Failed to initialize feature tables: {str(e)}")
        raise


def preprocess_trip_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform taxi trip data."""
    try:
        if df.empty:
            return pd.DataFrame()

        # Convert to Spark DataFrame
        spark_df = spark.createDataFrame(df)

        # Calculate trip duration and speed
        spark_df = spark_df.withColumn(
            "trip_duration_minutes",
            F.round((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60, 2)
        )
        
        spark_df = spark_df.withColumn(
            "avg_speed_mph",
            F.when(F.col("trip_duration_minutes") > 0,
                  F.round(F.col("trip_distance") / (F.col("trip_duration_minutes") / 60), 2)
            ).otherwise(0)
        )

        # Flag shared rides and suspicious trips
        spark_df = spark_df.withColumn(
            "is_shared_ride",
            F.col("passenger_count") > 1
        )

        spark_df = spark_df.withColumn(
            "is_suspicious",
            (F.col("trip_duration_minutes") < 1) |
            (F.col("avg_speed_mph") > 80) |
            (F.col("trip_distance") == 0) |
            (F.col("fare_amount") == 0)
        )

        # Data quality filters
        spark_df = spark_df.filter(
            F.col("trip_id").isNotNull() &
            F.col("vendor_id").isNotNull() &
            F.col("pickup_datetime").isNotNull() &
            F.col("dropoff_datetime").isNotNull() &
            F.col("passenger_count") > 0 &
            F.col("trip_distance") >= 0 &
            F.col("fare_amount") >= 0
        )

        # Convert back to pandas
        df = spark_df.toPandas()

        # Write to offline store
        if not df.empty:
            postgres_writer.write_batch(
                df=df,
                table_name="trip_features",
                if_exists="append",
                chunk_size=10000,
            )
            logger.info(f"Wrote {len(df)} trip records to offline store")

        return df

    except Exception as e:
        logger.exception(f"Error in preprocess_trip_data: {str(e)}")
        return pd.DataFrame()


def preprocess_location_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform location update data."""
    try:
        if df.empty:
            return pd.DataFrame()

        # Convert to Spark DataFrame
        spark_df = spark.createDataFrame(df)

        # Data quality filters
        spark_df = spark_df.filter(
            F.col("location_id").isNotNull() &
            F.col("latitude").between(-90, 90) &
            F.col("longitude").between(-180, 180) &
            F.col("current_demand") >= 0 &
            F.col("available_taxis") >= 0 &
            F.col("surge_multiplier") >= 1.0
        )

        # Convert back to pandas
        df = spark_df.toPandas()

        # Write to offline store
        if not df.empty:
            postgres_writer.write_batch(
                df=df,
                table_name="location_features",
                if_exists="append",
                chunk_size=10000,
            )
            logger.info(f"Wrote {len(df)} location records to offline store")

        return df

    except Exception as e:
        logger.exception(f"Error in preprocess_location_data: {str(e)}")
        return pd.DataFrame()


class TaxiTripsProcessor(SparkKafkaProcessor):
    """Process taxi trip data stream."""
    def _ingest_stream_data(self):
        logger.debug("Starting trip data ingestion...")
        stream_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TRIPS_TOPIC)
            .load()
        )

        parsed_df = stream_df.select(
            F.from_json(F.col("value").cast("string"), TRIP_SCHEMA).alias("data")
        ).select("data.*")

        # Just add watermark without any sorting
        return parsed_df.withWatermark("event_timestamp", "5 minutes")

    def process_batch(self, df: pd.DataFrame, batch_id) -> pd.DataFrame:
        """Process each micro-batch"""
        try:
            if df.empty:
                logger.warning(f"Batch {batch_id}: Empty DataFrame received")
                return df

            # Convert timestamp column if it's string
            if 'event_timestamp' in df.columns and df['event_timestamp'].dtype == 'object':
                df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])

            # Apply preprocessing
            if self.preprocess_fn:
                df = self.preprocess_fn(df)

            return df

        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {str(e)}")
            return df


class LocationUpdatesProcessor(SparkKafkaProcessor):
    """Process location updates stream."""
    def _ingest_stream_data(self):
        logger.debug("Starting location updates ingestion...")
        stream_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_LOCATIONS_TOPIC)
            .load()
        )

        parsed_df = stream_df.select(
            F.from_json(F.col("value").cast("string"), LOCATION_SCHEMA).alias("data")
        ).select("data.*")

        # Just add watermark without any sorting
        return parsed_df.withWatermark("event_timestamp", "1 minute")

    def process_batch(self, df: pd.DataFrame, batch_id) -> pd.DataFrame:
        """Process each micro-batch"""
        try:
            if df.empty:
                logger.warning(f"Batch {batch_id}: Empty DataFrame received")
                return df

            # Convert timestamp column if it's string
            if 'event_timestamp' in df.columns and df['event_timestamp'].dtype == 'object':
                df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])

            # Apply preprocessing
            if self.preprocess_fn:
                df = self.preprocess_fn(df)

            return df

        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {str(e)}")
            return df


def run_materialization():
    """Run Feast materialize_incremental every hour."""
    while True:
        try:
            logger.info("Starting materialization job...")
            store.materialize_incremental(end_date=datetime.utcnow())
            logger.info("Materialization completed successfully.")
        except Exception as e:
            logger.error(f"Materialization failed: {e}")
        time.sleep(3600)


if __name__ == "__main__":
    try:
        # Initialize feature tables
        initialize_feature_tables()

        # Get feature views
        trip_features_view = store.get_stream_feature_view("realtime_trips_stream")
        location_features_view = store.get_stream_feature_view("location_updates_stream")

        # Configure processors - removed invalid watermark_delay_ms parameter
        processor_config = SparkProcessorConfig(
            mode="spark",
            source="kafka",
            spark_session=spark,
            processing_time="4 seconds",
            query_timeout=30
        )

        # Set Spark session configurations for watermarking
        spark.conf.set("spark.sql.streaming.watermarkDelayThreshold", "5 minutes")
        spark.conf.set("spark.sql.streaming.minBatchesToRetain", "2")

        # Initialize processors
        trip_processor = TaxiTripsProcessor(
            config=processor_config,
            fs=store,
            sfv=trip_features_view,
            preprocess_fn=preprocess_trip_data,
        )

        # location_processor = LocationUpdatesProcessor(
        #     config=processor_config,
        #     fs=store,
        #     sfv=location_features_view,
        #     preprocess_fn=preprocess_location_data,
        # )

        # Start ingestion
        logger.info("Starting feature ingestion...")
        trip_query = trip_processor.ingest_stream_feature_view(PushMode.ONLINE)
        # location_query = location_processor.ingest_stream_feature_view(PushMode.ONLINE)

        # Wait for termination
        trip_query.awaitTermination()
        # location_query.awaitTermination()

    except Exception as e:
        logger.error(f"Failed to start feature ingestion: {str(e)}")
        raise
