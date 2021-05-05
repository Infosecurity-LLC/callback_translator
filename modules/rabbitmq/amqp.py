import logging
from typing import Dict

import pika

logger = logging.getLogger('callback_translator')


def create_rmq_connection(settings: Dict) -> pika.adapters.blocking_connection.BlockingConnection:
    settings = settings.copy()
    username = settings.pop('username', None)
    password = settings.pop('password', None)
    parameters = pika.ConnectionParameters(
        credentials=pika.credentials.PlainCredentials(username, password), **settings
    )
    connection = pika.BlockingConnection(parameters)
    logger.info("Create rmq connection channel with parameters %s", str(parameters))
    return connection


def prepare_producer_channel(connection: pika.adapters.blocking_connection.BlockingConnection,
                             exchange_name: str) -> pika.adapters.blocking_connection.BlockingChannel:
    channel = connection.channel()

    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)
    logger.info("Declare rmq exchange %s for producer channel %s", exchange_name, str(channel))
    return channel


def prepare_consumer_channel(connection: pika.adapters.blocking_connection.BlockingConnection, exchange_name: str,
                             queue_name: str) -> pika.adapters.blocking_connection.BlockingChannel:
    channel = connection.channel()

    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)
    logger.info("Declare rmq exchange %s for consumer channel %s", exchange_name, str(channel))

    channel.queue_declare(queue=queue_name, durable=True)
    logger.info("Declare rmq queue %s for consumer channel %s", queue_name, str(channel))

    channel.queue_bind(exchange=exchange_name, queue=queue_name)
    logger.info("Bind queue %s to exchange %s", queue_name, exchange_name)

    channel.basic_qos(prefetch_count=1)
    return channel
