from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

class ItemViewStreamEvent(BaseModel):
    itemId: Optional[str]
    deviceId: Optional[str] 
    ipAddress: Optional[str] 
    timestamp: Optional[str]
    viewId: Optional[str]

@app.post('/')
async def main(event: ItemViewStreamEvent):
    print(event)
    return {'Message': 'Successful post'}