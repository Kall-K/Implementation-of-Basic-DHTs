import time

from network import PastryNetwork
from node import PastryNode
from constants import *


def main():
    # Create the Pastry network
    print("Creating the Pastry network...")
    network = PastryNetwork()

    # Predefined node IDs to test
    predefined_ids = ["4b12", "fa35", "19bd", "4bde", "4c12", "cafe"]

    print(f"Adding {len(predefined_ids)} nodes to the network...")

    for node_id in predefined_ids:
        # Create a PastryNode with a specific ID
        node = PastryNode(network, node_id=node_id)
        node.start_server()
        time.sleep(1)  # Sleep for a second to allow the server to start
        network.node_join(node)
        print(f"Node Added: ID = {node.node_id}, Address = {node.address}")

    # Print the state of each node
    print("\nInspecting the state of each node:")
    for node in network.nodes.values():
        node.print_state()


if __name__ == "__main__":
    main()
