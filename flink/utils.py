import os
from pyflink.datastream import StreamExecutionEnvironment
import subprocess

def setup_flink_env() -> StreamExecutionEnvironment:
    """Setup Flink environment with required JARs"""
    # Set specific Java options for PyFlink
    os.environ["PYFLINK_CLIENT_JAVA_OPTS"] = "--add-opens=java.base/java.net=ALL-UNNAMED"
    
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Get the project root directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define JAR paths
    jars_dir = os.path.join(current_dir, "jars")
    kafka_connector_jar = os.path.join(jars_dir, "flink-connector-kafka-1.17.1.jar")
    kafka_clients_jar = os.path.join(jars_dir, "kafka-clients-3.4.0.jar")
    
    # Check if JARs exist
    required_jars = [
        (kafka_connector_jar, "Kafka connector"),
        (kafka_clients_jar, "Kafka client")
    ]
    
    for jar_path, jar_name in required_jars:
        if not os.path.exists(jar_path):
            raise FileNotFoundError(f"{jar_name} JAR not found at {jar_path}")
        env.add_jars(f"file://{jar_path}")
    
    return env