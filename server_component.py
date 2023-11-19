import redis
from fastapi import FastAPI, Query, HTTPException
from typing import Optional
import logging

from redis_conf_reader import parse_redis_conf
from storage_component import StorageInterface
from redis_config import *
import os

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# redis_master_config = parse_redis_conf('redis-master.conf')
# redis_slave1_config = parse_redis_conf('redis-slave1.conf')
# redis_slave2_config = parse_redis_conf('redis-slave2.conf')


storage = StorageInterface(logger, redis_master_config, [redis_slave1_config, redis_slave2_config])

@app.get('/ping_redis')
async def ping_redis():
    try:
        client = redis.Redis(host='master', port=6379, password='password')
        response = client.ping()
        print("Подключение успешно:", response)
        return True
    except Exception as e:
        print("Ошибка подключения:", e)
        return False


@app.get("/keys/{key}")
async def read_key(key: str):
    value = storage.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": value}


@app.put("/keys/{key}")
async def write_key(key: str, value: str = Query(...)):
    storage.set(key, value)
    return {"status": "success"}

