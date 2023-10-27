from fastapi import FastAPI, Query, HTTPException
from typing import Optional
import logging
from storage_component import StorageInterface
import os

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
storage = StorageInterface(logger)




@app.get("/keys/{key}")
async def read_key(key: str):
    value = storage.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": value}


@app.put("/keys/{key}")
async def write_key(key: str, value: str = Query(...)):
    storage.set(key=key, value=value)
    return {"status": "success"}

