import asyncio
import json
import time

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from aiokafka.errors import UnknownMemberIdError
from aiokafka.errors import UnknownTopicOrPartitionError
from loguru import logger

from config import KAFKA_CONFIG, APP_CONFIG
from utils.email import send_normal_mail, send_activation_mail, send_reset_mail, send_newpasswd_mail
from utils.mintnft import async_nft_mintnft
# from utils.log import log as logger

KAFKA_BOOTSTRAP_SERVERS = KAFKA_CONFIG['address']
KAFKA_TOPIC = KAFKA_CONFIG['topic']
KAFKA_CONSUMER_GROUP = KAFKA_CONFIG['group']

# ========================================================== consume-func

# mail
async def kafka_send_email(offset, value):
    logger.debug(f"kafka_send_email offset: {offset} value: {value}")
    if value['subject'] == 'activation':
        result = send_activation_mail(value['to_email'], value['username'], value['context'])
    elif value['subject'] == 'reset':
        result = send_reset_mail(value['to_email'], value['username'], value['context'])
    elif value['subject'] == 'newpasswd':
        result = send_newpasswd_mail(value['to_email'], value['username'], value['context'])
    else:
        result = send_normal_mail(value['to_email'], value['subject'], value['context'])
    return result

# mintnft
async def kafka_update_mintnft(offset, value):
    logger.debug(f"kafka_update_mintnft offset: {offset} value: {value}")
    await async_nft_mintnft(value)

# ========================================================== consume topic

## 消费者队列 topic-mail
async def kafka_consume_mail():
    topics = [KAFKA_TOPIC + 'mail']
    consumer = None
    while True:
        try:
            if consumer is None:
                logger.debug(f"kafka_consume_mail KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
                consumer = AIOKafkaConsumer(*topics,
                                    loop=asyncio.get_event_loop(),
                                    session_timeout_ms=30000,  # 30s
                                    heartbeat_interval_ms=10000,
                                    auto_commit_interval_ms=7000,
                                    # max_poll_interval_ms=86400000,
                                    group_id=KAFKA_CONSUMER_GROUP,
                                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

            # get cluster layout and join group KAFKA_CONSUMER_GROUP
            await consumer.start()

            # consume messages
            async for message in consumer:
                logger.debug(f"consume message: {message}")
                offset = message.offset
                value = json.loads(message.value)
                logger.debug(f"kafka_consume_mail offset: {offset} value: {value}")

                result = await kafka_send_email(offset=offset,value=value,)

                if result and result['code'] != 200:
                    logger.error(f"kafka_consume_mail to: {value['to_email']} result: {str(result)}")
                    await consumer.stop()
                    consumer = None
                    break  # 退出循环，重新启动消费者
                else:
                    # 打印邮件延迟时间
                    delay_timestamp = int(time.time()) - int(message.timestamp / 1000)
                    logger.warning(f"kafka_consume_mail to: {value['to_email']}  delay: {delay_timestamp} seconds")
                # Commit the consumed messages
                await consumer.commit()
                
                if value['subject'] in ['wakeup','activity']: # 活动/唤醒 邮件需等待2秒
                    await asyncio.sleep(2)
                
            print(f"kafka_consume_mail end")
        except (KafkaError, UnknownMemberIdError, UnknownTopicOrPartitionError) as e:
            logger.error(f"kafka_consume_mail Kafka error: {str(e)}")
            if consumer and not consumer._closed:
                await consumer.stop()
            consumer = None
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"kafka_consume_mail Exception error: {str(e)}")
            if consumer and not consumer._closed:
                await consumer.stop()
            consumer = None
            await asyncio.sleep(60)
        finally:
            # will leave consumer group; perform autocommit if enabled.
            if consumer and not consumer._closed:
                await consumer.stop()
            consumer = None

# 消费者队列 topic-mintnft
async def kafka_consume_mintnft():
    topics = [KAFKA_TOPIC + 'mintnft']
    while True:
        try:
            logger.debug(f"kafka_consume_mintnft KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
            consumer = AIOKafkaConsumer(*topics,
                                    loop=asyncio.get_event_loop(),
                                    session_timeout_ms=30000,  # 30s
                                    heartbeat_interval_ms=10000,
                                    auto_commit_interval_ms=7000,
                                    # max_poll_interval_ms=86400000,
                                    group_id=KAFKA_CONSUMER_GROUP,
                                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                    )

            # get cluster layout and join group KAFKA_CONSUMER_GROUP
            await consumer.start()

            # consume messages
            async for message in consumer:
                # logger.debug(f"consume message: {message}")
                offset = message.offset
                value = json.loads(message.value)
                logger.debug(f"kafka_consume_mintnft offset: {offset} value: {value}")
                
                await kafka_update_mintnft(offset=offset, value=value)

                # Commit the consumed messages
                await consumer.commit()
            print(f"kafka_consume_mintnft end")
        except (KafkaError, UnknownMemberIdError, UnknownTopicOrPartitionError) as e:
            logger.error(f"kafka_consume_mintnft Kafka error: {str(e)}")
            await consumer.stop()
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"kafka_consume_mintnft Exception error: {str(e)}")
            await consumer.stop()
            await asyncio.sleep(5)
        finally:
            # will leave consumer group; perform autocommit if enabled.
            if not consumer._closed:
                await consumer.stop()

# ==========================================================
