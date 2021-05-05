import logging
from queue import Queue, Empty
from threading import Thread

from appmetrics import metrics
from pika.exceptions import AMQPConnectionError
from retry import retry
from typing import NoReturn, List

from pika.adapters.blocking_connection import BlockingChannel

from exception_thread import ExceptionThread
from exceptions import SettingTypeError
from modules.callback.flowable import flowable_callback
from modules.rabbitmq import Consumer

logger = logging.getLogger('callback_translator')


# connection attempt within 3 minutes (value of geometric progression sum,
# # where tries-1 - number of the last term, delay - first term, backoff - denominator)
@retry(AMQPConnectionError, tries=10, delay=1, backoff=1.71, logger=logger)
def create_consumer_threads(number_consumer_threads: int):
    consumer_threads = []
    exception_bucket = Queue()
    consumers = []
    for __ in range(number_consumer_threads):
        consumer = Consumer()
        consumer.create_channel()
        thread = ExceptionThread(
            target=receive,
            args=(consumer.queue_name, consumer.channel),
            daemon=True,
            exception_bucket=exception_bucket
        )
        consumers.append(consumer)
        consumer_threads.append(thread)

    for thread in consumer_threads:
        thread.start()
        thread.name = "consumer_thread"
        logger.info("Start %s", str(thread))

    return consumer_threads, exception_bucket


def recreate_connection(target_exception_bucket: Queue, consumer_threads: List[Thread], number_consumer_threads: int):
    while True:
        try:
            target_exc = target_exception_bucket.get(block=False)
        except Empty:
            pass
        else:
            metrics.notify('consumer_errors', 1)
            logger.error("All consumers should be stopped by this exception: %s", str(target_exc))

            for thread in consumer_threads:
                thread.join()

            logger.info("Trying to recreate connection with RabbitMQ")
            consumer_threads, target_exception_bucket = create_consumer_threads(number_consumer_threads)


def receive(queue_name: str, channel: BlockingChannel) -> NoReturn:
    channel.basic_consume(queue=queue_name, on_message_callback=flowable_callback)
    channel.start_consuming()
    logger.warning("Exception!")


def target_processing(number_consumer_threads: int):
    if not isinstance(number_consumer_threads, int):
        raise SettingTypeError(f"Setting {number_consumer_threads} must be int")

    consumer_threads, consumer_exception_bucket = create_consumer_threads(number_consumer_threads)

    recreate_exception_bucket = Queue()
    connection_recreator_thread = ExceptionThread(
        target=recreate_connection,
        args=(consumer_exception_bucket, consumer_threads, number_consumer_threads),
        daemon=True,
        exception_bucket=recreate_exception_bucket
    )

    connection_recreator_thread.start()
    connection_recreator_thread.name = "connection_recreator"
    logger.info("Start %s thread", str(connection_recreator_thread))

    return recreate_exception_bucket
