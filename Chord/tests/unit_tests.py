import time

import threading
import pandas as pd

import os, sys

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from Chord.network import ChordNetwork
from Chord.node import ChordNode


def add_node_test():
    network = ChordNetwork()  # 2^16 = 65,536
    node = ChordNode(network, "4b12")  # 19,218
    node.start_server()
    network.node_join(node)
    assert len(network.nodes) == 1
    assert sum([1 for node_id in node.finger_table if node_id != "4b12"]) == 0

    node2 = ChordNode(network, "fa35")  # 64,053
    node2.start_server()
    network.node_join(node2)
    assert len(network.nodes) == 2
    print("Finger Tables after second node joins:")
    print(node.finger_table)
    print(node2.finger_table)
    time.sleep(6)
    print("\nFinger Tables after update:")
    print(node.finger_table)
    print(node2.finger_table)

    node3 = ChordNode(network, "19bd")  # 6,589
    node3.start_server()
    network.node_join(node3)
    assert len(network.nodes) == 3
    print("\nFinger Tables after third node joins:")
    print(node.finger_table)
    print(node2.finger_table)
    print(node3.finger_table)
    time.sleep(6)
    print("\nFinger Tables after update:")
    print(node.finger_table)
    print(node2.finger_table)
    print(node3.finger_table)
    time.sleep(6)
    print("\nFinger Tables after second update:")
    print(node.finger_table)
    print(node2.finger_table)
    print(node3.finger_table)

    print("\n===== END OF TEST =====\n")
    time.sleep(10)


add_node_test()
