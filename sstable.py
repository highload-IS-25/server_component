from pathlib import Path
from typing import Optional
from bisect import bisect


class SSTable:
    def __init__(self, filename, block_size):
        self.__filename = filename
        self.__hash = {}
        self.__block_size = block_size
        self.__folder = "storage_data"

    def create(self, memtable):
        # Specify the file path
        file_path = Path(self.__folder, self.__filename)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as out:

            data = list(memtable.data.items())
            self.__hash.update({data[0][0]: 0})
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
                    out.write(current_block_len.to_bytes(8, byteorder='big'))
                    out.write(current_block)  # вот тут нужно сжатие + как-то зафиксировать длину
                    current_block_len = 0  # сбрасываем длину блока
                    current_block = pair_string

                last_key_offset = last_key_offset + len(pair_string)
                current_block_len += len(pair_string)
                current_block.extend(pair_string)
            if current_block_len != 0:
                out.write(current_block_len.to_bytes(8, byteorder='big'))
                out.write(current_block)

    def search_for_item(self, key: str) -> Optional[str]:
        keys = list(self.__hash.keys())
        key_to_check = bisect(keys, key)
        key_to_check = keys[key_to_check - 1]
        path = Path(self.__folder, self.__filename)
        dictionary = {}
        with open(path, "rb") as f:
            f.seek(self.__hash[key_to_check])
            block_size = int.from_bytes(f.read(8), byteorder='big')
            f.seek(8)
            data = f.read(block_size)
            i = 0

            while i < len(data):
                key_size = int.from_bytes(data[i: i + 8], byteorder='big')
                key_f = data[i + 8: i + 8 + key_size].decode(encoding='utf-8')
                i += key_size + 8
                value_size = int.from_bytes(data[i: i + 8], byteorder='big')
                value_f = data[i + 8: i + 8 + value_size].decode(encoding='utf-8')
                dictionary[key_f] = value_f
                i += value_size + 8
        if key in dictionary:
            return dictionary[key]
        return None
