import json
import logging
import os
import time
from typing import Dict

import requests
from appmetrics import metrics
from retry import retry
from socutils import get_settings

from exceptions import SettingMissingError
from modules import path_to_settings

logger = logging.getLogger('callback_translator')


def get_flowable_url() -> str:
    settings = get_settings(path_to_settings)["FLOWABLE"]
    user = settings['user']
    password = settings['password']
    protocol = settings['protocol']
    host = settings['host']
    endpoint = 'flowable-task/process-api/runtime/process-instances'
    url = '{protocol}://{user}:{passw}@{host}/{endpoint}'.format(protocol=protocol, user=user, passw=password,
                                                                 host=host, endpoint=endpoint)
    return url


def collect_query(record: Dict) -> Dict:
    query_to_flowable = {
        "message": "new incident",
        "businessKey": record['objectId'],
        "variables": [
            {
                "name": "organization",
                "value": record['object']['customFields']['correlationEventCollectorOrganization']['string']
            },
            {
                "name": "body",
                "value": json.dumps(record)
            }
        ]}

    return query_to_flowable


def is_duplicate_found(response):
    return "act_uniq_hi_bus_key" in response.text


@metrics.with_histogram("flowable_sending_time", reservoir_type='sliding_time_window')
@retry(requests.RequestException, tries=-1, delay=2, backoff=1, logger=logger)
def post_request_to_flowable(url, json_data, proxies=None):
    response = requests.post(url, json=json_data, timeout=3, proxies=proxies)
    try:
        response.raise_for_status()
        logger.debug(response)
    except requests.exceptions.HTTPError:
        if is_duplicate_found(response):
            logger.warning("The parameter rootId must be unique for incident %s. The incident is not processed in "
                           "flowable", json_data["businessKey"])
            metrics.notify("incident_duplication_warnings", 1)
            return
    except requests.RequestException as exc:
        logger.warning('Exception raised by flowable request for incident %s. Exception: %s',
                       json.loads(json_data)["businessKey"], str(exc))
        metrics.notify("flowable_error", 1)
        raise exc

    logger.info("Send message to flowable: %s", json_data)
    metrics.notify('incidents_sent_flowable', 1)
    metrics.notify('successful_request_time', time.time())


def flowable_callback(ch, method, properties, body):
    logger.info("Received message from queue")

    try:
        url = get_flowable_url()
    except KeyError as e:
        raise SettingMissingError(f"Cant find value: {e}")

    body = json.loads(body)
    try:
        data_to_flowable = collect_query(body)
    except KeyError as err:
        logger.warning('No such parameters in json data: %s. \n Incident %s not sent to flowable', str(err),
                       json.dumps(body))
        metrics.notify("invalid_input_errors", 1)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    post_request_to_flowable(url, data_to_flowable)
    ch.basic_ack(delivery_tag=method.delivery_tag)
