import time
from typing import Optional

from mem_table import MemTable, SSTable
from types_enum import Type
from sstable import SSTable


class LSM:
    def __init__(self, time_merging: int, mem_table_size: int, file_size: int, mem_table_type: Type, block_size: int):
        self.mem_table = MemTable(tree_type=mem_table_type, block_size=block_size)
        self.segments_list = []
        self.mem_table_size = mem_table_size
        self.time_merging = time_merging  # ну не придумала как назвать!! это короче сколько он спит прежде чем мержит

    def add_item(self, key: str, value: str):
        if self.mem_table.get_size() == self.mem_table_size:
            new_segment = self.mem_table.write_to_file()
            self.segments_list.append(new_segment)
        self.mem_table.write(key, value)

    def get_item(self, key: str) -> Optional[str]:
        item = self.mem_table.read(key)
        if item is None:
            for segment in reversed(self.segments_list):
                item = segment.search_for_item(key=key)
                if item is not None:
                    break
        return item

    def start(self):
        while True:
            time.sleep(self.time_merging)
            # тут смержить первые два, вставить получившийся в начало а потом видимо опять заснуть

    def merge(self, table, other_table):
        result = {}

        file1 = open(table.__filename)
        file2 = open(other_table.__filename)

        onstring1 = file1.read().split(";")[:-1]
        onstring2 = file2.read().split(";")[:-1]

        dict1 = dict()

        for item in onstring1:
            key = item.split(":")[0]
            value = item.split(":")[1]
            dict1[key] = value

        file1.close()

        dict2 = dict()

        for item in onstring2:
            key = item.split(":")[0]
            value = item.split(":")[1]
            dict2[key] = value

        file2.close()

        i, j = 0, 0
        while i < len(dict1) or j < len(dict2):
            if i < len(dict1) and (j == len(dict2) or dict1[i][0] < dict2[j][0]):
                result.update({dict1[i][0]: dict1[i][0]})
                i += 1
            else:
                if dict1[i][0] == dict2[j][0]:
                    i += 1
                result.update(dict2[j][0])
                j += 1

        return result  # из этой даты обратно создаем sstable
