from flink.utils import setup_flink_env
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
    DeliveryGuarantee
)

def test_kafka_sink():
    # Get configured environment
    env = setup_flink_env()
    
    try:
        sink = (
            KafkaSink.builder()
            .set_bootstrap_servers("localhost:9092")
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic("test-topic")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build()
        )
        print("Successfully created Kafka sink!")
        return sink
    except Exception as e:
        print(f"Error creating Kafka sink: {str(e)}")
        raise

if __name__ == "__main__":
    test_kafka_sink()
