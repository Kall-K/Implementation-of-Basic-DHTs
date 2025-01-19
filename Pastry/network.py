import matplotlib.pyplot as plt
import numpy as np

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

        # Find the closest node to the new using its position
        closest_node = self._find_topologically_closest_node(new_node)

        # Initialize the new nodes Neighborhood Set of the new node
        new_node.initialize_neighborhood_set(closest_node.node_id)

        # Forward the join message to the topologically closest node
        join_request = {
            "operation": "JOIN_NETWORK",
            "joining_node_id": new_node.node_id,
            "hops": [],
        }
        response = new_node.send_request(closest_node, join_request)
        print(response)

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

            distance = topological_distance(new_node.position, existing_node.position)
            if distance < min_distance:
                closest_node = existing_node
                min_distance = distance
        return closest_node

    def visualize_network(self, threshold=0.05):
        """
        Visualizes the Pastry network by placing nodes on a circular ring
        based on their 4-digit hex ID. Lower values are at the top (12 o’clock),
        and values increase clockwise, with FFFF also at the top.

        Nodes that are too close together will be moved outward slightly.
        """
        if not self.nodes:
            print("No nodes in the network to visualize.")
            return

        # Convert node IDs from hex to integers for sorting
        sorted_nodes = sorted(self.nodes.keys(), key=lambda x: int(x, 16))

        radius = 1  # Fixed radius for the ring
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.set_xlim(-1.2, 1.2)
        ax.set_ylim(-1.2, 1.2)

        # Draw the circular ring
        circle = plt.Circle((0, 0), radius, color="lightgray", fill=False)
        ax.add_patch(circle)

        placed_positions = []  # Store positions for overlap checking

        # Arrange nodes based on their numerical value
        for node_id in sorted_nodes:
            angle = 2 * np.pi * (int(node_id, 16) / 0xFFFF)  # Map ID to [0, 2π]
            base_x, base_y = radius * np.sin(angle), radius * np.cos(angle)

            # Check for overlap within the threshold distance
            shift = 0.03  # Base shift distance
            while any(
                np.linalg.norm([base_x - px, base_y - py]) < threshold
                for px, py in placed_positions
            ):
                base_x += shift * np.cos(angle)  # Move outward slightly
                base_y += shift * np.sin(angle)
                shift += 0.02  # Gradually increase shift distance

            placed_positions.append((base_x, base_y))  # Store final position

            ax.plot(base_x, base_y, "bo", markersize=10)  # Blue nodes
            ax.text(base_x, base_y, node_id, fontsize=10, ha="center", va="center", color="black")

        # Remove axis ticks and labels
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_xticklabels([])
        ax.set_yticklabels([])
        ax.set_title("Pastry Network Visualization (Nodes on a Ring)")

        plt.show()
