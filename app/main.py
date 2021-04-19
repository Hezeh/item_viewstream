from fastapi import FastAPI, Header
from pydantic import BaseModel
from typing import Optional
from google.cloud import  pubsub_v1
import os
import json
import firebase_admin
from firebase_admin import firestore, credentials

from confluent_kafka import Producer

app = FastAPI()

publisher = pubsub_v1.PublisherClient()
project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
# Use the application default credentials
cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred, {
  'projectId': project_id,
})

db = firestore.client()

conf = {
        'bootstrap.servers': os.environ.get('SERVERS'),
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': os.environ.get("USERNAME"),
        'sasl.password': os.environ.get("PASSWORD"),
    }

producer = Producer(conf)


class ItemViewStreamEvent(BaseModel):
    itemId: Optional[str]
    deviceId: Optional[str]
    timestamp: Optional[str]
    viewId: Optional[str]
    merchantId: Optional[str]
    searchQuery: Optional[str]
    index: Optional[str]
    type: Optional[str]

@app.post('/')
async def main(event: ItemViewStreamEvent, x_forwarded_for: Optional[str] = Header(None)):
    data = {
        "viewId": event.viewId,
        "deviceId" : event.deviceId,
        "itemId": event.itemId,
        "timestamp": event.timestamp,
        "merchantId": event.merchantId,
        "searchQuery": event.searchQuery,
        "index": event.index,
        "type": event.type,
    }
    data["ipAddress"] = str(x_forwarded_for)
    data = json.dumps(data).encode('utf-8')
    print(data)
    # Publish to Pub/Sub
    topic_name = f'projects/{project_id}/topics/item-viewstream'
    data = json.dumps(data).encode('utf-8')
    future = publisher.publish(topic_name, data)
    return future.result()