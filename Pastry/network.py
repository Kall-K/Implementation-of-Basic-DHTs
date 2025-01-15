from ipaddress import ip_address
from helper_functions import *


class PastryNetwork:
    def __init__(self):
        self.nodes = {}  # Dictionary. Keys are node IDs, values are Node objects

    def node_join(self, new_node):
        """
        Handles a new node joining the Pastry network.
        """
        # Determine the node ID
        node_id = new_node.node_id

        # Add the node to the network
        self.nodes[node_id] = new_node

        if len(self.nodes) == 1:
            print("The network is empty. The new node is the first node.")
            return

        # Find the closest node to the new using its IP address
        closest_node = self._find_topologically_closest_node(new_node)

        # Update the new nodes Neighborhood Set of the new node
        new_node.initialize_neighborhood_set(closest_node.node_id)

        # Forward the join message to the topologically closest node
        join_request = {
            "operation": "JOIN_NETWORK",
            "joining_node_id": new_node.node_id,
            "hops": [],
        }
        new_node.send_request(closest_node, join_request)

        # Broadcast the new node's arrival to the network
        new_node.transmit_state()

    def _find_topologically_closest_node(self, new_node):
        """
        Find the topologically closest node in the network to the new node.
        """
        closest_node = None
        min_distance = float("inf")
        for existing_node in self.nodes.values():
            # Skip the new node
            if existing_node == new_node:
                continue

            distance = topological_distance(
                new_node.address[0], existing_node.address[0]
            )
            if distance < min_distance:
                closest_node = existing_node
                min_distance = distance
        return closest_node
