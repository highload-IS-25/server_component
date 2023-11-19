import os
import random
from typing import Optional
import logging
from filelock import FileLock
import redis

class StorageInterface:
    def __init__(self, logger, master_config, replica_configs):
        self.__logger = logger
        self.master_client = redis.StrictRedis(**master_config, decode_responses=True)
        self.replica_clients = [redis.StrictRedis(**config, decode_responses=True) for config in replica_configs]

    def get(self, key: str) -> Optional[str]:
        try:
            replica = random.choice(self.replica_clients + [self.master_client])
            self.__logger.info(f"\nRead k:'{key}' from{replica.connection_pool.connection_kwargs['host']}")
            value = replica.get(key)

            if value is not None:
                self.__logger.info(f"\nRetrieved '{key}' with value: {value}")
            else:
                self.__logger.warning(f"\n Key '{key}' not found.")

            return value

        except Exception as e:
            self.__logger.error(f"Error reading key '{key}' from Redis: {e}")
            return None

    def set(self, key: str, value: str):
        try:
            self.__logger.info(f"Write key '{key}' with value '{value}'.")
            self.master_client.set(key, value)
        except Exception as e:
            self.__logger.error(f"Error writing key '{key}' with value '{value}' to Redis master: {e}")


