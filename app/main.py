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
topic = os.getenv('TOPIC_NAME')
topic_name = f'projects/{project_id}/topics/{topic}'

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

@app.post('/')
async def main(event: ItemViewStreamEvent, x_forwarded_for: Optional[str] = Header(None)):
    data = {
        "viewId": event.viewId,
        "deviceId" : event.deviceId,
        "itemId": event.itemId,
        "timestamp": event.timestamp,
        "merchantId": event.merchantId,
    }
    data["ipAddress"] = str(x_forwarded_for)
    data = json.dumps(data).encode('utf-8')
    # producer.produce(topic, key=event.viewId, value=data)
    # future = publisher.publish(topic_name, data)
    # Read current value from firestore, save
    doc_ref = db.collection(u'profile').document(f'{event.merchantId}')

    doc = doc_ref.get()
    if doc.exists:
        doc_data = doc.to_dict()
        json_dump = json.dumps(doc_data)
        json_doc_data = json.loads(json_dump)
        total_views = json_doc_data["totalViews"]
        new_total_views = int(total_views) + 1
        res = doc_ref.set({
            'totalViews': new_total_views
        }, merge=True)
        return res
    else:
        return {"Message": "No such document"}