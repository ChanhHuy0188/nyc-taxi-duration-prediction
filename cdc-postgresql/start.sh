bash run.sh register_connector ./configs/cdc-postgresql.json
python3 -m create_table
python3 -m insert_data
