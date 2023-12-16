from typing import Optional

from logger import Logger
from mem_table import MemTable, SSTable
from types_enum import Type
from sstable import SSTable
import asyncio
import os
from pathlib import Path


class LSM:
    def __init__(self, time_merging: int, mem_table_size: int, file_size: int, mem_table_type: Type, block_size: int):
        self.mem_table = MemTable(tree_type=mem_table_type, block_size=block_size)
        self.__block_size = block_size
        self.segments_list = []
        self.mem_table_size = mem_table_size
        self.file_size = file_size
        self.time_merging = time_merging  # ну не придумала как назвать!! это короче сколько он спит прежде чем мержит
        self.logger = Logger()
        for key, value in self.logger.recover():
            self.add_item(key, value)
        #self.start()

    def add_item(self, key: str, value: str):
        if self.mem_table.get_size() == self.mem_table_size:
            new_segment = self.mem_table.write_to_file()
            self.segments_list.append(new_segment)
            self.logger.clear_log()
        self.mem_table.write(key, value)
        self.logger.log_write(key, value)

    def get_item(self, key: str) -> Optional[str]:
        item = self.mem_table.read(key)
        if item is None:
            for segment in reversed(self.segments_list):
                item = segment.search_for_item(key=key)
                if item is not None:
                    break
        return item

    def merge(self, table, other_table):

        dictionary1 = table.get_all_content()
        dictionary2 = other_table.get_all_content()

        dict1 = list(dictionary1.items())
        dict2 = list(dictionary2.items())

        result = {}

        i, j = 0, 0
        while i < len(dict1) or j < len(dict2):
            if i < len(dict1) and (j == len(dict2) or dict1[i][0] < dict2[j][0]):
                result.update({dict1[i][0]: dict1[i][1]})
                i += 1
            else:
                if i < len(dict1) and j < len(dict2) and dict1[i][0] == dict2[j][0]:
                    i += 1
                result.update({dict2[j][0]: dict2[j][1]})
                j += 1

        return result  # из этой даты обратно создаем sstable

    async def auto_merge_task(self):
        while True:
            print("Quantity of segments now is: ", len(self.segments_list))
            if len(self.segments_list) > 1:
                print("Merging is happening!!")

                segment1 = self.segments_list[0]
                segment2 = self.segments_list[1]

                merge_result_dictionary = self.merge(segment1, segment2)
                # print("MERGE RESULT ", merge_result_dictionary)
                new_filename = segment1.filename

                new_sstable = SSTable("temp", self.__block_size)
                self.segments_list.insert(0, new_sstable)
                new_sstable.create_from_data(list(merge_result_dictionary.items()))

                self.segments_list.pop(1)
                self.segments_list.pop(1)

                os.remove(segment1.folder + "/" + segment1.filename)
                os.remove(segment2.folder + "/" + segment2.filename)

                temp_path = Path(segment1.folder, "temp")
                new_path = Path(segment1.folder, new_filename)

                os.rename(temp_path, new_path)
                new_sstable.filename = new_filename
                print("Merging is done yay!!!!")

            # тут смержить первые два, вставить получившийся в начало а потом видимо опять заснуть

            await asyncio.sleep(self.time_merging)


def chunk_dict(d, chunk_size):
    return [{k: d[k] for k in list(d.keys())[i:i+chunk_size]}
            for i in range(0, len(d), chunk_size)]