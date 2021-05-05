import json
import logging
from time import sleep
from typing import Dict

import requests
from appmetrics import metrics
from retry import retry

logger = logging.getLogger('callback_translator')


def get_rabbimq_url(settings: Dict) -> (str, requests.Session):
    rabbitmq_settings = settings["RABBITMQ"]["connection"]
    host = rabbitmq_settings['host']
    port = settings["RABBITMQ"]["api_port"]

    user = rabbitmq_settings['username']
    password = rabbitmq_settings['password']

    session = requests.Session()
    session.auth = (user, password)

    url = 'http://{host}:{port}/api/overview'.format(host=host, port=port)

    return url, session


# request attempt within 3 minutes (value of geometric progression sum,
# # where tries-1 - number of the last term, delay - first term, backoff - denominator)
@retry(requests.RequestException, tries=10, delay=1, backoff=1.71, logger=logger)
def counting_queue_size(url: str, session: requests.Session):
    try:
        response = session.get(url, timeout=3)
        response.raise_for_status()
        logger.debug(response)
    except requests.RequestException as exc:
        logger.warning('Exception raised by rabbitmq metric request: %s', str(exc))
        raise exc

    response = json.loads(response.content)

    metrics.notify("messages_acked", response["message_stats"].get('ack', 0))
    metrics.notify("messages_published", response["message_stats"].get("publish", 0))
    metrics.notify("messages_ready", response["queue_totals"].get("messages_ready", 0))
    metrics.notify("messages_unacked", response["queue_totals"].get("messages_unacknowledged"))


def get_rabbitmq_metric(settings):
    while True:
        url, session = get_rabbimq_url(settings)
        counting_queue_size(url, session)
        sleep(1)
