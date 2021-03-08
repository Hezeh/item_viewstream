from fastapi import FastAPI, Header
from pydantic import BaseModel
from typing import Optional
from google.cloud import  pubsub_v1
import os
import json

from confluent_kafka import Producer
import socket

app = FastAPI()

publisher = pubsub_v1.PublisherClient()
project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
topic = os.getenv('TOPIC_NAME')
topic_name = f'projects/{project_id}/topics/{topic}'

conf = {
        'bootstrap.servers': os.environ.get('SERVERS'),
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': os.environ.get("USERNAME"),
        'sasl.password': os.environ.get("PASSWORD"),
        'auto.offset.reset': 'earliest',
    }

producer = Producer(conf)


class ItemViewStreamEvent(BaseModel):
    itemId: Optional[str]
    deviceId: Optional[str] 
    ipAddress: Optional[str] 
    timestamp: Optional[str]
    viewId: Optional[str]

@app.post('/')
async def main(event: ItemViewStreamEvent, x_forwarded_for: Optional[str] = Header(None)):
    data = {
        "viewId": event.viewId,
        "deviceId" : event.deviceId,
        "itemId": event.itemId,
        "timestamp": event.timestamp,
    }
    data["ipAddress"] = str(x_forwarded_for)
    data = json.dumps(data).encode('utf-8')
    producer.produce(topic, key=event.viewId, value=data)
    future = publisher.publish(topic_name, data)
    return future.result()