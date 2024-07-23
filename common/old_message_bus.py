from pika import (
    PlainCredentials,
    ConnectionParameters,
    BlockingConnection,
    exceptions,
)
from functools import wraps
from common.config import RABBIT_MQ


class MessageBus:
    def __init__(self):
        self.channel = None
        self.service_callback_handler = None
        self.user = RABBIT_MQ["UserName"]
        self.password = RABBIT_MQ["Password"]
        self.server = RABBIT_MQ["Host"]
        self.port = RABBIT_MQ["Port"]
        self.virtual_host = RABBIT_MQ["VirtualHost"]

    def _reconnect_if_required(func):
        @wraps(func)
        def _func_wrapper(*args, **kwargs):
            self = args[0]
            try:
                return func(*args, **kwargs)
            except exceptions.StreamLostError:
                print("Connection lost. Reconnecting ...")
                self.connect()
                return func(*args, **kwargs)

        return _func_wrapper

    def connect(self):
        """Connect to RabbitMQ. Raise Exception if failed"""
        try:
            credentials = PlainCredentials(self.user, self.password)
            parameters = ConnectionParameters(
                self.server, self.port, self.virtual_host, credentials, heartbeat=20000)
            connection = BlockingConnection(parameters)
            self.channel = connection.channel()
            self.channel.basic_qos(prefetch_count=1)
        except Exception:
            print("Could not connect to the Message Bus")
            raise

    @_reconnect_if_required
    def make_queue_durable(self, queue):
        """Makes a queue durable. This will ensure that RabbitMQ will never lose our queue"""
        self.channel.queue_declare(queue=queue, durable=True)

    @_reconnect_if_required
    def send(self, queue, msg, correlation_id: str = None, exchange: str = ""):
        """Sends a message to a specified queue"""
        if queue:
            self.make_queue_durable(queue)
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=queue,
                body=msg
            )

    @_reconnect_if_required
    def start_consuming(self, queue, service_callback_handler, exchange=None):
        """
        Starts consuming to a specified queue and calls method callbackHandler for callback
        """
        self.make_queue_durable(queue)
        if exchange:
            self.channel.queue_bind(exchange=exchange, queue=queue)
        self.service_callback_handler = service_callback_handler
        self.channel.basic_consume(
            queue, self.service_callback_handler, auto_ack=False)
        print(f"waiting for messages from {queue}..")
        self.channel.start_consuming()

    def stop(self):
        """Stop Complete Channel"""
        self.channel.close()

    @_reconnect_if_required
    def acknowledge_message(self, ch, method):
        """Acknowledge specified Message"""
        print("message is processed")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    @_reconnect_if_required
    def not_acknowledge_message(self, ch, method, requeue):
        """Requeue the message"""
        print("message will be rejected")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)
