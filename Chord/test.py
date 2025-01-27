import time
import threading

from network import ChordNetwork
from node import ChordNode
from constants import *


def main():
    # Create the Chord network
    print("Creating the Chord network...")
    network = ChordNetwork()

    # Predefined node IDs to test
    predefined_ids = ["4b12", "fa35", "19bd", "4bde", "4c12", "cafe"]

    print(f"Adding {len(predefined_ids)} nodes to the network...")


    def visualize_network():
        time.sleep(6)
        network.visualize_network()
    server_thread = threading.Thread(target=visualize_network, daemon=True)
    server_thread.start()

    
    for node_id in predefined_ids:
        # Create a ChordNode with a specific ID
        node = ChordNode(network, node_id=node_id)
        node.start_server()
        time.sleep(1)  # Allow the server to start
        network.node_join(node)
        # print(f"Node Added: ID = {node.node_id}, Port = {node.port}")
        #node.print_state()





    # for node_id in predefined_ids:
    #     # Create a ChordNode with a specific ID
    #     node = ChordNode(network, node_id=node_id)
    #     node.start_server()
    #     time.sleep(1)  # Allow the server to start
    #     network.node_join(node)
    #     print(f"Node Added: ID = {node.node_id}, Address = {node.address}")
    #     #node.print_state()


if __name__ == "__main__":
    main()