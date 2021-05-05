from modules.rabbitmq.amqp import create_rmq_connection, prepare_consumer_channel
from modules.rabbitmq.settings_handler import SettingsHandler


class Consumer(SettingsHandler):
    """
    Consumer to read messages from RabbitMQ.
    """
    def __init__(self, channel=None):
        super().__init__()

        self.channel = channel

    def create_channel(self):
        """
        Create channel for consumer.
        """
        connection = create_rmq_connection(settings=self.connection_settings)
        channel = prepare_consumer_channel(connection=connection, exchange_name=self.exchange_name,
                                           queue_name=self.queue_name)
        self.channel = channel
