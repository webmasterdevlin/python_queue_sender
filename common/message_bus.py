from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiver, ServiceBusReceivedMessage, \
    ServiceBusReceiveMode
from functools import wraps
from typing import Callable, Optional
from common.config import AZURE_SERVICE_BUS


class MessageBus:
    """
    A class to manage message bus operations using Azure Service Bus.

    Attributes:
        service_callback_handler (Optional[Callable[[ServiceBusReceivedMessage], None]]): The callback function to handle messages.
        connection_str (str): Connection string for the Azure Service Bus.
        queue_name (str): The name of the queue.
        servicebus_client (Optional[ServiceBusClient]): The Azure Service Bus client instance.
    """

    def __init__(self):
        """
        Initializes the MessageBus with connection parameters from the configuration.
        """
        self.service_callback_handler: Optional[Callable[[ServiceBusReceivedMessage], None]] = None
        self.connection_str: str = AZURE_SERVICE_BUS["ConnectionString"]
        self.queue_name: str = AZURE_SERVICE_BUS["QueueName"]
        self.servicebus_client: Optional[ServiceBusClient] = None

    def _reconnect_if_required(func: Callable) -> Callable:
        """
        A decorator to handle reconnection if the connection to the service bus is lost.

        Args:
            func (Callable): The function to wrap.

        Returns:
            Callable: The wrapped function.
        """

        @wraps(func)
        def _func_wrapper(*args, **kwargs):
            self = args[0]
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"Error: {e}. Attempting to reconnect...")
                self.connect()
                return func(*args, **kwargs)
        return _func_wrapper

    def connect(self) -> None:
        """
        Connect to Azure Service Bus. Raise an exception if the connection fails.
        """
        try:
            self.servicebus_client = ServiceBusClient.from_connection_string(self.connection_str)
            print("Connected to Azure Service Bus")
        except Exception:
            print("Could not connect to the Azure Service Bus")
            raise

    @_reconnect_if_required
    def make_queue_durable(self, queue: str) -> None:
        """
        Ensure the queue is configured to be durable in Azure Service Bus.

        Args:
            queue (str): The name of the queue to make durable.
        """
        print(f"Ensuring queue {queue} is durable (this is handled via Azure configurations).")

    @_reconnect_if_required
    def send(self, queue: str, msg: str, correlation_id: Optional[str] = None) -> None:
        """
        Sends a message to the specified queue.

        Args:
            queue (str): The name of the queue.
            msg (str): The message to send.
            correlation_id (Optional[str]): The correlation ID for the message. Defaults to None.
        """
        self.make_queue_durable(queue)
        with self.servicebus_client.get_queue_sender(queue_name=queue) as sender:
            service_bus_message = ServiceBusMessage(msg, message_id=correlation_id)
            sender.send_messages(service_bus_message)
            print(f"Message sent to queue {queue}")

    @_reconnect_if_required
    def start_consuming(self, queue: str,
                        service_callback_handler: Callable[[ServiceBusReceivedMessage], None]) -> None:
        """
        Starts consuming messages from the specified queue and calls the callback handler.

        Args:
            queue (str): The name of the queue.
            service_callback_handler (Callable[[ServiceBusReceivedMessage], None]): The callback function to handle messages.
        """
        self.make_queue_durable(queue)
        self.service_callback_handler = service_callback_handler
        with self.servicebus_client.get_queue_receiver(queue_name=queue,
                                                       receive_mode=ServiceBusReceiveMode.PEEK_LOCK) as receiver:
            print(f"Waiting for messages from {queue}...")
            for msg in receiver:
                try:
                    self.service_callback_handler(msg)
                    receiver.complete_message(msg)
                    print("Message processed and acknowledged")
                except Exception as e:
                    print(f"Message processing failed: {e}")
                    receiver.abandon_message(msg)

    def stop(self) -> None:
        """
        Close the service bus client connection.
        """
        self.servicebus_client.close()
        print("Service bus client closed")

    @_reconnect_if_required
    def acknowledge_message(self, receiver: ServiceBusReceiver, message: ServiceBusReceivedMessage) -> None:
        """
        Acknowledge that the message has been processed.

        Args:
            receiver (ServiceBusReceiver): The receiver object.
            message (ServiceBusReceivedMessage): The message to acknowledge.
        """
        try:
            receiver.complete_message(message)
            print("Message acknowledged")
        except Exception as e:
            print(f"Failed to acknowledge message: {e}")
            raise

    @_reconnect_if_required
    def not_acknowledge_message(self, receiver: ServiceBusReceiver, message: ServiceBusReceivedMessage,
                                requeue: bool = True) -> None:
        """
        Reject the message and optionally requeue it.

        Args:
            receiver (ServiceBusReceiver): The receiver object.
            message (ServiceBusReceivedMessage): The message to reject.
            requeue (bool, optional): Whether to requeue the message. Defaults to True.
        """
        try:
            if requeue:
                receiver.abandon_message(message)
                print("Message requeued")
            else:
                receiver.dead_letter_message(message)
                print("Message dead-lettered")
        except Exception as e:
            print(f"Failed to not acknowledge message: {e}")
            raise
