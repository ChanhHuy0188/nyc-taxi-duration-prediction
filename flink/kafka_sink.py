from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
    DeliveryGuarantee
)
from typing import Optional
import logging

logger = logging.getLogger(__name__)

def build_sink(topic: str, bootstrap_servers: str) -> Optional[KafkaSink]:
    """
    Create a Kafka sink with error handling
    """
    try:
        logger.info(f"Creating Kafka sink for topic: {topic}")
        print(f"Creating Kafka sink for topic: {topic}")
        sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(bootstrap_servers)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )
        logger.info("Kafka sink created successfully")
        return sink
    except Exception as e:
        logger.error(f"Failed to create Kafka sink: {str(e)}", exc_info=True)
        raise
