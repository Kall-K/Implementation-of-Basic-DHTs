from ipaddress import ip_address
from helper_functions import *


class ChordNetwork:
    bootstrap_node = None

    def __init__(self):
        self.nodes = {}  # Dictionary. Keys are node IDs, values are Node objects
        self.used_ports = []

    def node_join(self, new_node):
        """
        Handles a new node joining the Chord network.
        """
        # Determine the node ID
        node_id = new_node.node_id

        # Add the node to the network
        self.nodes[node_id] = new_node

        # # Add the node's port to the node_ports dictionary
        # self.node_ports[new_node_id] = new_node.port

        if len(self.nodes) == 1:
            print("The network is empty. This node is the first node.")
            ChordNetwork.bootstrap_node = new_node
            self.successor = self
            return

        successor_id = new_node.request_find_successor(node_id, ChordNetwork.bootstrap_node)
        # new_node joins on successor
        new_node.join(self.nodes[successor_id])
