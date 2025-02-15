import argparse
import json
import os
import shutil

def main(args):
    # Clean up the avro schema folder if exists,
    # then create a new folder
    if os.path.exists(args["schema_folder"]):
        shutil.rmtree(args["schema_folder"])
    os.mkdir(args["schema_folder"])

    # Create the taxi trip schema
    schema = {
        "doc": "Schema for NYC Yellow Taxi Trip Data",
        "fields": [
            {"name": "VendorID", "type": "int"},
            {"name": "tpep_pickup_datetime", "type": "string"},
            {"name": "tpep_dropoff_datetime", "type": "string"},
            {"name": "passenger_count", "type": "int"},
            {"name": "trip_distance", "type": "double"},
            {"name": "pickup_longitude", "type": "double"},
            {"name": "pickup_latitude", "type": "double"},
            {"name": "RateCodeID", "type": "int"},
            {"name": "store_and_fwd_flag", "type": "string"},
            {"name": "dropoff_longitude", "type": "double"},
            {"name": "dropoff_latitude", "type": "double"},
            {"name": "payment_type", "type": "int"},
            {"name": "fare_amount", "type": "double"},
            {"name": "extra", "type": "double"},
            {"name": "mta_tax", "type": "double"},
            {"name": "tip_amount", "type": "double"},
            {"name": "tolls_amount", "type": "double"},
            {"name": "improvement_surcharge", "type": "double"},
            {"name": "total_amount", "type": "double"}
        ],
        "name": "TaxiTrip",
        "namespace": "com.nyc.taxi",
        "type": "record"
    }

    # Write this schema to the Avro output folder
    with open(f'{args["schema_folder"]}/green_tripdata_2024-01.parquet', "w+") as f:
        json.dump(schema, f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o",
        "--schema_folder",
        default="./avro_schemas",
        help="Folder containing all generated avro schemas",
    )
    args = vars(parser.parse_args())
    main(args)
