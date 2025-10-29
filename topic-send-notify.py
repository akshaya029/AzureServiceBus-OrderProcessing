"""
notify_order.py
Consumes messages from Azure Service Bus topic and sends notification emails via Mailtrap.
"""

from azure.servicebus import ServiceBusClient
from azure.servicebus.management import ServiceBusAdministrationClient
from azure.core.exceptions import ResourceNotFoundError
from dotenv import load_dotenv
import os, json, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time

load_dotenv()

# Azure Service Bus config
SERVICEBUS_CONN_STR = "Endpoint=sb://orderprocessingapp.servicebus.windows.net/;SharedAccessKeyName=OrderPolicy;SharedAccessKey=G3tPhqK1s3KVgx4cQCNLVXxxBt3Ap6yE4+ASbHtIIYw="
TOPIC_NAME = "order-topic"
SUBSCRIPTION_NAME = "OrderProcessing-Sub"

# Mailtrap credentials 
MAILTRAP_HOST = "ssandbox.smtp.mailtrap.io"
MAILTRAP_PORT = "2525"
MAILTRAP_USER = "448c24bec4a741"
MAILTRAP_PASS = "1405d518938c23"
SENDER_EMAIL = "eswariakshaya056@gmail.com"
if not SERVICEBUS_CONN_STR:
    raise SystemExit(" Please set SERVICEBUS_CONN_STR in .env file")


def ensure_topic_and_subscription():
    
    admin_client = ServiceBusAdministrationClient.from_connection_string(SERVICEBUS_CONN_STR)

    # Ensure topic
    try:
        admin_client.get_topic(TOPIC_NAME)
        print(f"Topic '{TOPIC_NAME}' already exists.")
    except ResourceNotFoundError:
        admin_client.create_topic(TOPIC_NAME)
        print(f"Created topic: {TOPIC_NAME}")

    # Ensure subscription
    try:
        admin_client.get_subscription(TOPIC_NAME, SUBSCRIPTION_NAME)
        print(f"Subscription '{SUBSCRIPTION_NAME}' already exists.")
    except ResourceNotFoundError:
        admin_client.create_subscription(TOPIC_NAME, SUBSCRIPTION_NAME)
        print(f"Created subscription: {SUBSCRIPTION_NAME}")


def send_email_notification(order):
    """Send notification email via Mailtrap"""
    subject = f"Order Confirmation - {order['order_id']}"
    body = f"""
    Hello,

    Your order has been successfully processed!

    Order ID: {order['order_id']}
    Total Amount: ₹{order['total']}
    Items: {order['items']}

    Thank you for shopping with us!

    -- Team Azure Order System
    """

    msg = MIMEMultipart()
    msg["From"] = "orders@example.com"
    msg["To"] = order["customer_email"]
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(MAILTRAP_HOST, MAILTRAP_PORT) as server:
            server.starttls()
            server.login(MAILTRAP_USER, MAILTRAP_PASS)
            server.sendmail(msg["From"], msg["To"], msg.as_string())
            print(f"Email sent successfully to {order['customer_email']}")
    except Exception as e:
        print(f"Failed to send email: {e}")


def main():
    ensure_topic_and_subscription()

    with ServiceBusClient.from_connection_string(SERVICEBUS_CONN_STR) as client:
        receiver = client.get_subscription_receiver(
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME
        )

        print(f"notifications on subscription: {SUBSCRIPTION_NAME}...")

        with receiver:
            for msg in receiver:
                try:
                    order = json.loads(str(msg))
                    send_email_notification(order)
                    receiver.complete_message(msg)
                except Exception as e:
                    print(f"Error processing notification: {e}")
                    receiver.abandon_message(msg)
                time.sleep(2)


if __name__ == "__main__":
    main()