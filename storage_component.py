import os
from typing import Optional
import logging
from filelock import FileLock
from lsm import LSM
from types_enum import Type
import asyncio

class StorageInterface:
    def __init__(self, logger):
        self.lsm = LSM(block_size=4000, time_merging=10, mem_table_size=2, file_size=400000, mem_table_type=Type.RBTREE)
        # asyncio.run(auto_merge(self.lsm))

    storage_dir = "data_storage"

    # @classmethod
    # def _get_file_path(cls, key: str) -> str:
    #     return os.path.join(cls.storage_dir, key)
    #
    # @classmethod
    # def _get_file_lock_path(cls, key: str) -> str:
    #     return f"{cls._get_file_path(key)}.lock"

    def get(self, key: str) -> Optional[str]:
        return self.lsm.get_item(key)
        # file_path = self._get_file_path(key)
        # lock = FileLock(self._get_file_lock_path(key))
        #
        # with lock:
        #     try:
        #         if os.path.exists(file_path):
        #             with open(file_path, 'r') as file:
        #                 return file.read()
        #     except Exception as e:
        #         self.__logger.error(f"Error reading the file: {e}")
        #
        # return None

    def set(self, key: str, value: str):
        self.lsm.add_item(key=key, value=value)
        # lock = FileLock(self._get_file_lock_path(key))
        #
        # with lock:
        #     try:
        #         os.makedirs(self.storage_dir, exist_ok=True)
        #         file_path = self._get_file_path(key)
        #         with open(file_path, 'w') as file:
        #             file.write(value)
        #     except Exception as e:
        #         self.__logger.error(f"Error writing to the file: {e}")
