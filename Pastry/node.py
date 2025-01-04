from Multidimensinal_Data_Structures.kd_tree import KDTree
from Multidimensinal_Data_Structures.lsh import LSH


class PastryNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.kd_tree = None  # KD-Tree for storing data points
