from bintrees import AVLTree
from bintrees import RBTree
from bintrees import FastBinaryTree

from types_enum import Type


class TreeFactory:
    def __init__(self, tree_type: Type):
        self.tree_type = tree_type

    def get_tree(self):
        if self.tree_type == Type.AVL:
            return AVLTree()
        elif self.tree_type == Type.RBTREE:
            return RBTree()
        else:
            return FastBinaryTree()
