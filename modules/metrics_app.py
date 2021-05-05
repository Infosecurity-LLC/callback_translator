import logging

from appmetrics import metrics
from appmetrics.histogram import SlidingTimeWindowReservoir

logger = logging.getLogger('callback_translator')


def register_metrics_app():
    metrics.new_counter("received_requests")
    metrics.new_counter("final_incidents")
    metrics.new_counter("incidents_sent_flowable")
    metrics.new_counter("consumer_errors")
    metrics.new_counter("producer_errors")
    metrics.new_counter("invalid_input_errors")
    metrics.new_gauge("successful_request_time")
    metrics.new_counter("incident_duplication_warnings")
    metrics.new_counter("flowable_error")
    metrics.new_counter("rabbitmq_metrics_error")
    if not metrics.REGISTRY.get("flowable_sending_time"):
        metrics.new_histogram("flowable_sending_time", SlidingTimeWindowReservoir())

    metrics.new_gauge("messages_acked")  # Count of messages confirmed
    metrics.new_gauge("messages_published")  # Count of messages published
    metrics.new_gauge("messages_ready")  # Count of messages ready for delivery
    metrics.new_gauge("messages_unacked")  # Count of unacknowledged messages

    metrics.tag("received_requests", "default")
    metrics.tag("final_incidents", "default")
    metrics.tag("incidents_sent_flowable", "default")
    metrics.tag("consumer_errors", "default")
    metrics.tag("producer_errors", "default")
    metrics.tag("invalid_input_errors", "default")
    metrics.tag("successful_request_time", "default")
    metrics.tag("incident_duplication_warnings", "default")
    metrics.tag("flowable_sending_time", "default")
    metrics.tag("flowable_error", "default")
    metrics.tag("rabbitmq_metrics_error", "default")

    metrics.tag("messages_acked", "default")
    metrics.tag("messages_published", "default")
    metrics.tag("messages_ready", "default")
    metrics.tag("messages_unacked", "default")

    logger.info("Register some metrics for app: %s", str(metrics.REGISTRY))
