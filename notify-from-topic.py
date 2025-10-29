"""
processor.py
Consumes messages from Azure Service Bus topic and processes orders.
"""

from azure.servicebus import ServiceBusClient
from azure.servicebus.management import ServiceBusAdministrationClient
from azure.core.exceptions import ResourceNotFoundError
from dotenv import load_dotenv
import os, json, time

load_dotenv()

SERVICEBUS_CONN_STR = "Endpoint=sb://orderprocessingapp.servicebus.windows.net/;SharedAccessKeyName=OrderPolicy;SharedAccessKey=G3tPhqK1s3KVgx4cQCNLVXxxBt3Ap6yE4+ASbHtIIYw="
TOPIC_NAME = "order-topic"
SUBSCRIPTION_NAME = "OrderProcessing-Sub"

if not SERVICEBUS_CONN_STR:
    raise SystemExit(" Please set SERVICEBUS_CONN_STR in .env file")


def ensure_topic_and_subscription():
  
    admin_client = ServiceBusAdministrationClient.from_connection_string(SERVICEBUS_CONN_STR)

    # Check topic existence
    try:
        admin_client.get_topic(TOPIC_NAME)
        print(f"Topic '{TOPIC_NAME}' already exists.")
    except ResourceNotFoundError:
        admin_client.create_topic(TOPIC_NAME)
        print(f"Created topic: {TOPIC_NAME}")

    # Check subscription existence
    try:
        admin_client.get_subscription(TOPIC_NAME, SUBSCRIPTION_NAME)
        print(f"Subscription '{SUBSCRIPTION_NAME}' already exists.")
    except ResourceNotFoundError:
        admin_client.create_subscription(TOPIC_NAME, SUBSCRIPTION_NAME)
        print(f"Created subscription: {SUBSCRIPTION_NAME}")


def process_order(order):
    """Simulate order processing"""
    print(f"\n Processing Order ID: {order['order_id']}")
    print(f"Items: {order['items']}")
    print(f"Total: ₹{order['total']}")
    print(f"Customer: {order['customer_email']}")
    print("-" * 60)
    time.sleep(2)


def main():
    ensure_topic_and_subscription()

    with ServiceBusClient.from_connection_string(SERVICEBUS_CONN_STR) as client:
        receiver = client.get_subscription_receiver(
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME
        )

        print(f" messages on subscription: {SUBSCRIPTION_NAME}...")

        with receiver:
            for msg in receiver:
                try:
                    order = json.loads(str(msg))
                    process_order(order)
                    receiver.complete_message(msg)
                except Exception as e:
                    print(f" Error processing message: {e}")
                    receiver.abandon_message(msg)


if __name__ == "__main__":
    main()