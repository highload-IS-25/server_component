from bintrees import AVLTree
from bintrees import RBTree
from bintrees import FastBinaryTree
from types_enum import Type
from tree_factory import TreeFactory
from sstable import SSTable
import datetime

class MemTable:

    def __init__(self, tree_type: Type, block_size: int):
        self.__size = 0
        self.tree_factory = TreeFactory(tree_type)
        self.data = self.tree_factory.get_tree()
        self.__block_size = block_size

    def write(self, key, value):
        self.data.insert(key, value)
        self.__size += 1
        # if self.data.__len__() + 1 > self.size:
        #     # вставить журналирование
        #     return 0
        # return 1

    def read(self, key):
        return self.data.get(key)

    def _empty(self):
        self.data = self.tree_factory.get_tree()
        self.__size = 0

    def get_size(self):
        return self.__size

    def write_to_file(self):
        current_time = datetime.datetime.now()
        time_string = current_time.strftime("%Y-%m-%d_%H-%M-%S")
        new_table = SSTable(filename=time_string, block_size=self.__block_size)
        new_table.create(self)
        self._empty()
        return new_table
