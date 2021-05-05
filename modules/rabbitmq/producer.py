import pika

from modules.rabbitmq.amqp import create_rmq_connection, prepare_producer_channel
from modules.rabbitmq.settings_handler import SettingsHandler


class Producer(SettingsHandler):
    """
    Producer for recording messages in RabbitMQ.
    """
    def __init__(self, channel=None):
        super().__init__()

        if channel is None:
            channel = self.create_channel()

        self.channel = channel

    def create_channel(self) -> pika.adapters.blocking_connection.BlockingChannel:
        """
        Create channel for producer.
        """
        connection = create_rmq_connection(settings=self.connection_settings)
        channel = prepare_producer_channel(connection=connection, exchange_name=self.exchange_name)

        return channel
