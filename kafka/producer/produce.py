import argparse
import json
import os
from time import sleep

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from schema_registry.client import SchemaRegistryClient, schema

load_dotenv()

OUTPUT_TOPICS = os.getenv("KAFKA_OUTPUT_TOPICS", "tracking.raw_user_behavior")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
SCHEMA_REGISTRY_SERVER = os.getenv(
    "KAFKA_SCHEMA_REGISTRY_URL", "http://schema-registry:8081"
)

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with driver stats events. Setup will teardown before beginning emitting events.",
)
parser.add_argument(
    "-b",
    "--bootstrap_servers",
    default=BOOTSTRAP_SERVERS,
    help="Where the bootstrap server is",
)
parser.add_argument(
    "-s",
    "--schema_registry_server",
    default=SCHEMA_REGISTRY_SERVER,
    help="Where to host schema",
)
parser.add_argument(
    "-c",
    "--avro_schemas_path",
    default=os.path.join(os.path.dirname(__file__), "avro_schemas"),
    help="Folder containing all generated avro schemas",
)

args = parser.parse_args()

# Define some constants
NUM_DEVICES = 1


def create_topic(admin, topic_name):
    # Create topic if not exists
    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=12, replication_factor=1)
        admin.create_topics([topic])
        print(f"A new topic {topic_name} has been created!")
    except Exception:
        print(f"Topic {topic_name} already exists. Skipping creation!")
        pass


def format_record(row, parsed_avro_schema):
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

        record = {
            "VendorID": safe_int(row.get("VendorID")),
            "tpep_pickup_datetime": str(pickup_datetime),
            "tpep_dropoff_datetime": str(dropoff_datetime),
            "passenger_count": safe_int(row.get("passenger_count")),
            "trip_distance": safe_float(row.get("trip_distance")),
            "pickup_longitude": safe_float(row.get("pickup_longitude")),
            "pickup_latitude": safe_float(row.get("pickup_latitude")),
            "RateCodeID": safe_int(row.get("RateCodeID")),
            "store_and_fwd_flag": safe_string(row.get("store_and_fwd_flag")),
            "dropoff_longitude": safe_float(row.get("dropoff_longitude")),
            "dropoff_latitude": safe_float(row.get("dropoff_latitude")),
            "payment_type": safe_int(row.get("payment_type")),
            "fare_amount": safe_float(row.get("fare_amount")),
            "extra": safe_float(row.get("extra")),
            "mta_tax": safe_float(row.get("mta_tax")),
            "tip_amount": safe_float(row.get("tip_amount")),
            "tolls_amount": safe_float(row.get("tolls_amount")),
            "improvement_surcharge": safe_float(row.get("improvement_surcharge")),
            "total_amount": safe_float(row.get("total_amount"))
        }

        formatted_record = {
            "schema": {"type": "struct", "fields": parsed_avro_schema["fields"]},
            "payload": record,
        }
        return json.dumps(formatted_record)
    except Exception as e:
        print(f"Error formatting record: {str(e)}, row: {row}")
        # Return record with default values if there's an error
        default_record = {
            "VendorID": 0,
            "tpep_pickup_datetime": str(pd.Timestamp.now()),
            "tpep_dropoff_datetime": str(pd.Timestamp.now()),
            "passenger_count": 0,
            "trip_distance": 0.0,
            "pickup_longitude": 0.0,
            "pickup_latitude": 0.0,
            "RateCodeID": 0,
            "store_and_fwd_flag": "N",
            "dropoff_longitude": 0.0,
            "dropoff_latitude": 0.0,
            "payment_type": 0,
            "fare_amount": 0.0,
            "extra": 0.0,
            "mta_tax": 0.0,
            "tip_amount": 0.0,
            "tolls_amount": 0.0,
            "improvement_surcharge": 0.0,
            "total_amount": 0.0
        }
        formatted_record = {
            "schema": {"type": "struct", "fields": parsed_avro_schema["fields"]},
            "payload": default_record,
        }
        return json.dumps(formatted_record)


def create_streams(servers, avro_schemas_path, schema_registry_client):
    producer = None
    admin = None

    # Add retry logic for Kafka connection
    for _ in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=str.encode,  # Simple string encoding
                batch_size=16384,  # Increase batch size (default 16384)
                buffer_memory=33554432,  # 32MB buffer memory
                compression_type="gzip",  # Enable compression
                linger_ms=50,  # Wait up to 50ms to batch messages
                acks=1,  # Only wait for leader acknowledgment
            )
            admin = KafkaAdminClient(bootstrap_servers=servers)
            print("SUCCESS: instantiated Kafka admin and producer")
            break
        except Exception as e:
            print(
                f"Trying to instantiate admin and producer with bootstrap servers {servers} with error {e}"
            )
            sleep(10)
            pass

    # Add retry logic for schema registry
    for _ in range(10):
        try:
            schema_registry_client.get_subjects()
            print("SUCCESS: connected to schema registry")
            break
        except Exception as e:
            print(
                f"Failed to connect to schema registry: {e}. Retrying in 10 seconds..."
            )
            sleep(10)
    else:
        raise Exception("Failed to connect to schema registry after 10 attempts")

    try:
        # Load the Avro schema
        avro_schema_path = f"{avro_schemas_path}/nyc_taxi_events.avsc"
        with open(avro_schema_path, "r") as f:
            parsed_avro_schema = json.loads(f.read())

        # Load data and prepare for batch processing
        print(f"Loading data from parquet file...")
        df = pd.read_parquet("data/green_tripdata_2024-01.parquet")
        
        # Replace infinite values with NaN
        df = df.replace([np.inf, -np.inf], np.nan)
        
        print(f"Loaded {len(df)} records from parquet file")

        # Process records in parallel using all available CPU cores
        print("Formatting records in parallel...")
        records = df.apply(lambda row: format_record(row, parsed_avro_schema), axis=1).tolist()
        print(f"Formatted {len(records)} records")

        # Get topic name and create it if needed
        topic_name = OUTPUT_TOPICS
        create_topic(admin, topic_name=topic_name)

        # Register schema if needed
        schema_version_info = schema_registry_client.check_version(
            f"{topic_name}-schema", schema.AvroSchema(parsed_avro_schema)
        )
        if schema_version_info is not None:
            schema_id = schema_version_info.schema_id
            print(f"Found existing schema ID: {schema_id}. Skipping creation!")
        else:
            schema_id = schema_registry_client.register(
                f"{topic_name}-schema", schema.AvroSchema(parsed_avro_schema)
            )
            print(f"Registered new schema with ID: {schema_id}")

        # Batch send records
        print("Starting to send records...")
        valid_records = 0
        invalid_records = 0

        for i, record in enumerate(records):
            try:
                producer.send(topic_name, value=record)
                valid_records += 1

                # Only print progress every 1000 records
                if i % 1000 == 0:
                    print(f"Sent {i} records (Valid: {valid_records}, Invalid: {invalid_records})")

                sleep(0.05)
            except Exception as e:
                print(f"Error sending record: {str(e)}")
                invalid_records += 1

        # Make sure all messages are sent
        producer.flush()
        print(f"Finished sending records - Total: {len(records)}, Valid: {valid_records}, Invalid: {invalid_records}")

    except Exception as e:
        print(f"Error in data streaming: {str(e)}")
    finally:
        if producer:
            producer.close()
        if admin:
            admin.close()


def teardown_stream(topic_name, servers=["localhost:9092"]):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        print(admin.delete_topics([topic_name]))
        print(f"Topic {topic_name} deleted")
    except Exception as e:
        print(str(e))
        pass


if __name__ == "__main__":
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    servers = parsed_args["bootstrap_servers"]
    schema_registry_server = parsed_args["schema_registry_server"]

    # Tear down all previous streams
    print("Tearing down all existing topics!")
    for device_id in range(NUM_DEVICES):
        try:
            teardown_stream(OUTPUT_TOPICS, [servers])
        except Exception as e:
            print(f"Topic device_{device_id} does not exist. Skipping...! {e}")

    if mode == "setup":
        avro_schemas_path = parsed_args["avro_schemas_path"]
        schema_registry_client = SchemaRegistryClient(url=schema_registry_server)
        create_streams([servers], avro_schemas_path, schema_registry_client)
