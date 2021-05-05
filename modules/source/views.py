import logging

from appmetrics import metrics
from flask import request
from flask.views import MethodView

from modules.source.webhook_controller import webhook

logger = logging.getLogger('callback_translator')


class WebHookListener(MethodView):
    def post(self) -> (str, int):
        metrics.notify('received_requests', 1)
        logger.info("Request for /webhook received")
        try:
            body = request.get_json()
        except Exception as e:
            logger.error(f'Input data not a json: %s', str(e))
            metrics.notify("invalid_input_errors", 1)
            return "Data not a json", 400

        server_answer = webhook(body)
        if server_answer:
            return 'Webhook done successfully', 201
        else:
            return 'Webhook did not post data', 202
