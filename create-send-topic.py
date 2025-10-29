"""
producer.py
Creates an order message and sends it to Azure Service Bus topic.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from dotenv import load_dotenv
import os, json

load_dotenv()

SERVICEBUS_CONN_STR = "xxxxxxxxxxxxxxxxxxxxx"
TOPIC_NAME = "xxxxxxxxxx"
if not SERVICEBUS_CONN_STR:
    raise SystemExit("Please set SERVICEBUS_CONN_STR in .env file")

app = FastAPI(title="Order Producer API")

class Order(BaseModel):
    order_id: str
    customer_email: str
    items: list[str]
    total: float

@app.post("/send-order")
async def send_order(order: Order):
    """Send order to Azure Service Bus topic"""
    try:
        with ServiceBusClient.from_connection_string(SERVICEBUS_CONN_STR) as client:
            sender = client.get_topic_sender(topic_name=TOPIC_NAME)
            with sender:
                message = ServiceBusMessage(json.dumps(order.dict()))
                sender.send_messages(message)
                print(f" Order sent to topic: {order.order_id}")
        return {"status": "success", "message": f"Order {order.order_id} sent successfully"}
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))