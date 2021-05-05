import json
import logging
from typing import Dict

from appmetrics import metrics
from pika import BasicProperties
from pika.exceptions import AMQPConnectionError
from retry import retry

from modules.rabbitmq import Producer

logger = logging.getLogger('callback_translator')


def is_final_incident(incident: Dict) -> bool:
    tags = incident['details'].get('tags', [])
    obj_type = incident.get('objectType')
    op = incident.get('operation')

    return 'FINAL' in tags and obj_type == 'case' and op == 'Update'


# connection attempt within a minute (value of geometric progression sum,
# where tries-1 - number of the last term, delay - first term, backoff - denominator)
@retry(AMQPConnectionError, tries=10, delay=1, backoff=1.44, logger=logger)
def publish(data: Dict) -> bool:
    producer = Producer()
    producer.channel.basic_publish(
        exchange=producer.exchange_name,
        routing_key='',
        body=json.dumps(data),
        properties=BasicProperties(delivery_mode=2)
    )

    logger.info("Send incident to RabbitMQ exchange %s", producer.exchange_name)
    producer.channel.connection.close()
    return True


def webhook(data: Dict) -> bool:
    if not is_final_incident(data):
        return False
    metrics.notify('final_incidents', 1)
    logger.info("Incident is final")

    try:
        is_published = publish(data)
    except Exception as e:
        metrics.notify('producer_errors', 1)
        logger.error("Error writing incident %s\n to RabbitMQ: %s", json.dumps(data), str(e))
        raise SystemExit

    return is_published
