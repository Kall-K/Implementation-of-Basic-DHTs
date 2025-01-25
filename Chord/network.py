from ipaddress import ip_address
from helper_functions import *


class ChordNetwork:
    bootstrap_node = None

    def __init__(self):
        self.nodes = {}  # Dictionary. Keys are node IDs, values are Node objects

    def node_join(self, new_node):
        """
        Handles a new node joining the Chord network.
        """
        # Determine the node ID
        node_id = new_node.node_id

        # Add the node to the network
        self.nodes[node_id] = new_node

        if len(self.nodes) == 1:
            print("The network is empty. This node is the first node.")
            ChordNetwork.bootstrap_node = new_node
            self.successor = self
            return

        # get_successor_request = {
        #     "operation": "FIND_SUCCESSOR",
        #     "node_id": node_id,
        # }
        # # Get the possition on the ring
        # successor_id = new_node.send_request(ChordNetwork.bootstrap_node, get_successor_request)
        successor_id = new_node.find_successor(node_id, ChordNetwork.bootstrap_node)
        print("THE SUCCESSOR IS: ", successor_id)
        # new_node joins on successor
        new_node.join(self.nodes[successor_id])
