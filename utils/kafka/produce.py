import json
import asyncio
from loguru import logger
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from aiokafka.errors import UnknownMemberIdError
from aiokafka.errors import UnknownTopicOrPartitionError

from config import KAFKA_CONFIG

KAFKA_BOOTSTRAP_SERVERS=KAFKA_CONFIG['address']
KAFKA_TOPIC=KAFKA_CONFIG['topic']

# ========================================================== kafka-produce-topic

## 生产者队列
async def kafka_send_produce(topic_key, json_value):
    logger.debug(f"kafka_produce KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
    producer = AIOKafkaProducer(
                                loop=asyncio.get_event_loop(), 
                                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                request_timeout_ms=30000  # 30s
                            )
    try:
        logger.debug(f"kafka_produce KAFKA_TOPIC: {KAFKA_TOPIC+topic_key}")
        # get cluster layout and initial topic/partition leadership information
        await producer.start()

        # produce message
        logger.debug(f"kafka_produce json_value: {json_value}")
        json_value_bytes = json.dumps(json_value).encode('utf-8')
        await producer.send_and_wait(KAFKA_TOPIC+topic_key, json_value_bytes)
    except (KafkaError, UnknownMemberIdError, UnknownTopicOrPartitionError) as e:
        logger.error(f"kafka_produce Kafka error: {str(e)}")
        await producer.stop()
    except Exception as e:
        logger.error(f"kafka_produce Exception error: {str(e)}")
        await producer.stop()
    finally:
        # will leave consumer group; perform autocommit if enabled.
        logger.success(f"kafka_produce message - json_value: {json_value}")
        if not producer._closed:
            await producer.stop()
