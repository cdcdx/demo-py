#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import asyncio
import sys
import threading

from utils.kafka.consume import kafka_consume_mail, kafka_consume_mintnft
from utils.log import log as logger

from config import KAFKA_CONFIG

## kafka-consume
def kafka_consume(topic):
    if topic == 'mail':
        logger.info(f"kafka_consume_mail")
        asyncio.run(kafka_consume_mail())
    elif topic == "mintnft":
        logger.info(f"kafka_consume_mintnft")
        asyncio.run(kafka_consume_mintnft())
    else:
        logger.error(f"unknown topic: {topic}")


## mutil-thread
async def mutil_thread():
    topics = KAFKA_CONFIG['topiclist']
    threads = []
    for topic in topics:
        thread = threading.Thread(target=kafka_consume, args=(topic,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    # 初始化参数
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", type=bool, default=False, action=argparse.BooleanOptionalAction)
    parser.add_argument("-l", "--log", type=str, default="warn")
    args = parser.parse_args()
    run_debug = bool(args.debug)
    run_log = str(args.log.lower())

    # 日志级别
    if run_debug:
        log_level = "DEBUG"
    else:
        if run_log == "debug":
            log_level = "DEBUG"
        elif run_log == "info":
            log_level = "INFO"
        elif run_log == "warn":
            log_level = "WARNING"
        elif run_log == "error":
            log_level = "ERROR"
        else:
            log_level = "WARNING"

    logger.remove()
    logger.add(sys.stdout, level=log_level)

    # print("mutil_thread")
    asyncio.run(mutil_thread())
