from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from google.cloud import  pubsub_v1
import os
import json

app = FastAPI()

publisher = pubsub_v1.PublisherClient()
project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
topic = os.getenv('TOPIC_NAME')
topic_name = f'projects/{project_id}/topics/{topic}'


class ItemViewStreamEvent(BaseModel):
    itemId: Optional[str]
    deviceId: Optional[str] 
    ipAddress: Optional[str] 
    timestamp: Optional[str]
    viewId: Optional[str]

@app.post('/')
async def main(event: ItemViewStreamEvent):
    data = {
        "viewId": event.viewId,
        "deviceId" : event.deviceId,
        "itemId": event.itemId,
        "ipAddress": event.ipAddress,
        "timestamp": event.timestamp,
    }
    data = json.dumps(data).encode('utf-8')
    future = publisher.publish(topic_name, data)
    return future.result()