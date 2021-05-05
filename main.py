import logging
from queue import Queue, Empty

from appmetrics import metrics
from socutils import get_settings

from exception_thread import ExceptionThread
from modules.autodiscovery.rabbitmq_metric import get_rabbitmq_metric
from modules.logging import prepare_logging
from modules.source.app import app_processing
from modules.target import target_processing

logger = logging.getLogger('callback_translator')


def thread_health_control(source_exception_bucket: Queue, target_exception_bucket: Queue,
                          rabbimq_metric_exception_bucket: Queue):
    while True:
        try:
            target_exc = target_exception_bucket.get(block=False)
        except Empty:
            pass
        else:
            logger.error("All should be stopped by this exception: %s", str(target_exc))
            raise target_exc

        try:
            source_exc = source_exception_bucket.get(block=False)
        except Empty:
            pass
        else:
            logger.error("All should be stopped by this exception: %s", str(source_exc))
            raise source_exc

        try:
            rabbimq_exc = rabbimq_metric_exception_bucket.get(block=False)
        except Empty:
            pass
        else:
            metrics.notify("rabbitmq_metrics_error", 1)
            logger.error("All should be stopped by this exception: %s", str(rabbimq_exc))
            raise rabbimq_exc


def main(path_to_settings: str = 'data/settings.yaml'):
    settings = get_settings(path_to_settings)
    prepare_logging(settings)

    source_exception_bucket = Queue()
    app_creator_thread = ExceptionThread(
        target=app_processing,
        args=(settings,),
        daemon=True,
        exception_bucket=source_exception_bucket
    )

    app_creator_thread.name = "connection_recreator"

    app_creator_thread.start()
    logger.info("Start %s", str(app_creator_thread))

    number_consumer_threads = settings["NUMBER_CONSUMER_THREADS"]
    target_exception_bucket = target_processing(number_consumer_threads)

    rabbimq_metric_exception_bucket = Queue()
    rabbimq_metric_thread = ExceptionThread(
        target=get_rabbitmq_metric,
        args=(settings,),
        daemon=True,
        exception_bucket=rabbimq_metric_exception_bucket
    )

    rabbimq_metric_thread.name = "rabbimq_metric"

    rabbimq_metric_thread.start()
    logger.info("Start %s", str(rabbimq_metric_thread))

    thread_health_control(source_exception_bucket=source_exception_bucket,
                          target_exception_bucket=target_exception_bucket,
                          rabbimq_metric_exception_bucket=rabbimq_metric_exception_bucket)


if __name__ == '__main__':
    from modules import path_to_settings as main_path_to_settings

    main(main_path_to_settings)
