from typing import Dict, Any

from socutils import get_settings

from exceptions import SettingMissingError, SettingTypeError

from modules import path_to_settings as modules_path_to_settings


class SettingsHandler:
    """
    Store and validate settings for interacting with RabbitMQ from the setting file.
    """

    def __init__(self, path_to_settings=modules_path_to_settings):
        self.settings = get_settings(path_to_settings)
        self.connection_settings = None
        self.exchange_name = None
        self.queue_name = None
        self.init_settings()

    def init_settings(self):
        """
        Init the Handler by using the given configuration.
        """
        connection_settings, exchange_name, queue_name = self.validate_args()
        self.connection_settings = connection_settings
        self.exchange_name = exchange_name
        self.queue_name = queue_name

    def validate_args(self) -> (Dict[str, Any], str, str):
        """
        Validates values​from a config

        :return:
        connection_settings - set of settings for connecting to RabbitMQ
        exchange_name - name of the exchange point RabbitMQ
        queue_name - RabbitMQ queue name
        """
        rabbitmq_settings = self.settings.get("RABBITMQ")
        if rabbitmq_settings is None:
            raise SettingMissingError("Missing rabbitmq parameter")
        if not isinstance(rabbitmq_settings, dict):
            raise SettingTypeError(f"Setting {rabbitmq_settings} must be dict")

        connection_settings = rabbitmq_settings.get("connection")
        if connection_settings is None:
            raise SettingMissingError("Missing rabbitmq.connection parameter")
        if not isinstance(connection_settings, dict):
            raise SettingTypeError(f"Setting {connection_settings} must be dict")

        exchange_name = rabbitmq_settings.get("exchange_name")
        if exchange_name is None:
            raise SettingMissingError("Missing rabbitmq.exchange_name parameter")
        if not isinstance(exchange_name, str):
            raise SettingTypeError(f"Setting {exchange_name} must be str")

        queue_name = rabbitmq_settings.get("queue_name")
        if queue_name is None:
            raise SettingMissingError("Missing rabbitmq.queue_name parameter")
        if not isinstance(queue_name, str):
            raise SettingTypeError(f"Setting {queue_name} must be str")

        return connection_settings, exchange_name, queue_name
