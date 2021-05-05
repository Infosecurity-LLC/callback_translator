import logging
from typing import Dict

import cheroot.wsgi
from flask import Flask

from modules.metrics_app import register_metrics_app
from modules.source.views import WebHookListener

logger = logging.getLogger('callback_translator')


def create_source_app(settings: Dict) -> Flask:
    from appmetrics.wsgi import AppMetricsMiddleware
    register_metrics_app()
    flask_app = Flask(__name__)
    flask_app.wsgi_app = AppMetricsMiddleware(flask_app.wsgi_app, "app_metrics")

    flask_app.config.from_mapping(settings)
    flask_app.add_url_rule('/webhook', view_func=WebHookListener.as_view('WebhookListener'))

    from modules.autodiscovery.health import health_bp
    flask_app.register_blueprint(health_bp)

    return flask_app


def app_run(app: Flask):
    server = cheroot.wsgi.Server(('0.0.0.0', app.config['APP_PORT']), app)
    try:
        logger.info('Start CherryPy WSGI server on http://{host}:{port}. Press Ctrl+C to stop'.format(
            host='0.0.0.0',
            port=app.config['APP_PORT']
        ))
        server.safe_start()
    except (KeyboardInterrupt, SystemExit) as e:
        server.stop()
        raise Exception


def app_processing(settings: Dict):
    app = create_source_app(settings)
    app_run(app)
