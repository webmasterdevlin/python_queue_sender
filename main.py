import asyncio
import json
from common.config import AZURE_SERVICE_BUS
from common.message_bus import MessageBus

# The interval in seconds between sending messages
interval: int = 5
# A counter to keep track of the number of messages sent
counter: int = 1


def send_json_message(msg_bus: MessageBus) -> None:
    """
    Sends a JSON message to the configured Azure Service Bus queue.

    Args:
        msg_bus (MessageBus): The message bus instance to use for sending the message.
    """
    global counter
    # Convert the JSON object to a string
    message_body = json.dumps({"id": f"object {counter}"})
    # Send the message to the configured queue
    msg_bus.send(AZURE_SERVICE_BUS["QueueName"], message_body)
    print(f"Send message({counter}) is done.")


async def main_loop() -> None:
    """
    The main loop that continuously sends messages to the Azure Service Bus queue.

    This function connects to the message bus and then enters an infinite loop,
    sending a message every `interval` seconds.

    The counter is incremented with each message sent.
    """
    # Instantiate the MessageBus
    msg_bus = MessageBus()
    msg_bus.connect()

    # Loop forever
    while True:
        send_json_message(msg_bus)
        global counter  # use the global counter variable
        counter += 1
        await asyncio.sleep(interval)  # wait for the specified interval before sending the next message


# Entry point of the application
if __name__ == "__main__":
    # Run the main loop until it is complete
    asyncio.run(main_loop())
