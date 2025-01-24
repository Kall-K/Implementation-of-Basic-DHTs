import time
import pandas as pd

import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from network import PastryNetwork
from node import PastryNode
from constants import *


def main():
    print("Stage 1: Node Joining")
    print("=======================")
    print("Creating the Pastry network...")
    network = PastryNetwork()

    # 12 Predefined node IDs. As many as the countries in the dataset
    predefined_ids = [
        "4b12",
        "fa35",
        "19bd",
        "37de",
        "3722",
        "ca12",
        "cafe",
        "fb32",
        "20bc",
        "20bd",
        "3745",
        "d3ad",
    ]

    print(f"Adding {len(predefined_ids)} nodes to the network...")
    for node_id in predefined_ids:
        node = PastryNode(network, node_id=node_id)
        node.start_server()
        time.sleep(0.1)  # Allow the server to start
        network.node_join(node)
        print(f"Node Added: ID = {node.node_id}, Position = {node.position}")
    print("\nAll nodes have successfully joined the network.\n")

    print("\nInspecting the state of each node:")
    for node in network.nodes.values():
        node.print_state()

    network.visualize_network()

    network.visualize_topology()


if __name__ == "__main__":
    main()
