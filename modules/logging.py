import logging
from typing import Dict, NoReturn

from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler

logger = logging.getLogger('callback_translator')


def prepare_logging(settings: Dict) -> NoReturn:
    basic_level = settings['logging']['basic_level']
    term_level = settings['logging']['term_level']
    logger.setLevel(basic_level)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(term_level)
    stream_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)-1s - %(message)s')
    )
    logger.addHandler(stream_handler)
    if settings.get('sentry_url'):
        sentry_handler = SentryHandler(settings['sentry_url'])
        sentry_handler.setLevel(settings['logging']['sentry_level'])
        setup_logging(sentry_handler)
        logger.addHandler(sentry_handler)
