from pathlib import Path
from typing import Optional
from bisect import bisect
import zlib
import os

class SSTable:
    def __init__(self, filename, block_size):
        self.filename = filename
        self.__hash = {}
        self.__block_size = block_size
        self.folder = "storage_data"

    def create(self, memtable):
        data = list(memtable.data.items())
        # print("WERE CREATING FROM MEM TABLE AND DATA IS ", data)
        self.create_from_data(data)

    def create_from_data(self, data):
        # Specify the file path
        file_path = Path(self.folder, self.filename)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as out:

            # self.__hash.update({data[0][0]: 0})
            self.__hash[data[0][0]] = 0
            key = data[0][0].encode('utf-8')
            value = data[0][1].encode('utf-8')

            pair_string = bytearray(len(key).to_bytes(8, byteorder='big'))
            pair_string.extend(key)
            pair_string.extend(len(value).to_bytes(8, byteorder='big'))
            pair_string.extend(value)
            last_key_offset = 0
            current_block = pair_string
            current_block_len = len(current_block)

            for i in range(1, len(data)):
                key = data[i][0].encode('utf-8')
                value = data[i][1].encode('utf-8')
                pair_string = bytearray(len(key).to_bytes(8, byteorder='big'))
                pair_string.extend(key)
                pair_string.extend(len(value).to_bytes(8, byteorder='big'))
                pair_string.extend(value)
                if current_block_len + len(pair_string) > self.__block_size:  # проверяем что вышли за блок
                    self.__hash.update({data[i - 1][0]: last_key_offset})  # в хэш записываем предыдущий
                    current_block = zlib.compress(current_block)
                    current_block_len = len(current_block)
                    out.write(current_block_len.to_bytes(8, byteorder='big'))
                    out.write(current_block)  # вот тут нужно сжатие + как-то зафиксировать длину
                    current_block_len = 0  # сбрасываем длину блока
                    current_block = pair_string

                last_key_offset = last_key_offset + len(pair_string)
                current_block_len += len(pair_string)
                current_block.extend(pair_string)
            if current_block_len != 0:
                current_block = zlib.compress(current_block)
                current_block_len = len(current_block)
                out.write(current_block_len.to_bytes(8, byteorder='big'))
                out.write(current_block)

    def get_block_content(self, start):
        path = Path(self.folder, self.filename)

        with open(path, "rb") as f:
            f.seek(start)
            dictionary = self.get_block_content_from_file(f)
        return dictionary

    def get_block_content_from_file(self, f):

        dictionary = {}
        block_size = int.from_bytes(f.read(8), byteorder='big')
        if block_size == 0:
            return {}
        # f.seek(8)
        # print(block_size, self.filename)
        data = f.read(block_size)
        data = zlib.decompress(data)
        i = 0

        while i < len(data):
            key_size = int.from_bytes(data[i: i + 8], byteorder='big')
            key_f = data[i + 8: i + 8 + key_size].decode(encoding='utf-8')
            i += key_size + 8
            value_size = int.from_bytes(data[i: i + 8], byteorder='big')
            value_f = data[i + 8: i + 8 + value_size].decode(encoding='utf-8')
            dictionary[key_f] = value_f
            i += value_size + 8
        f.seek(block_size, 1)
        return dictionary

    def get_all_content(self):
        path = Path(self.folder, self.filename)
        dictionary = {}
        with open(path, "rb") as f:
            while True:
                new_content = self.get_block_content_from_file(f)
                if f.tell() == os.path.getsize(path) or len(new_content) == 0:
                    break
                dictionary = {**dictionary, **new_content}
        return dictionary


    def search_for_item(self, key: str) -> Optional[str]:
        keys = list(self.__hash.keys())
        key_to_check = bisect(keys, key)
        key_to_check = keys[key_to_check - 1]
        dictionary = self.get_block_content(self.__hash[key_to_check])

        if key in dictionary:
            return dictionary[key]
        return None

