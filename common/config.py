import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

AZURE_SERVICE_BUS = {
    "ConnectionString": os.getenv("SERVICE_BUS_CONNECTION_STR"),
    "QueueName": os.getenv("SERVICE_BUS_QUEUE_NAME"),
}

RABBIT_MQ = {
    "UserName": 'admin',
    "Password": '${1}',
    "Port": 5672,
    "VirtualHost": "/",
    "Host": '10.231.91.4'
}
