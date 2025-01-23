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

    def visualize_network(self, threshold=0.2):
        """
        Visualizes the Pastry network by placing nodes on a circular ring
        based on their 4-digit hex ID. Lower values are at the top (12 o'clock),
        and values increase clockwise.

        Nodes that are too close together will be moved slightly.
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

        placed_positions = {}  # Store positions for overlap checking

        # Arrange nodes based on their numerical value
        for node_id in sorted_nodes:
            angle = 2 * np.pi * (int(node_id, 16) / 0xFFFF)
            base_x, base_y = radius * np.sin(angle), radius * np.cos(angle)

            # Check for overlap within the threshold distance
            shift_angle = np.radians(6)  # Base shift distance
            for close_node_id in placed_positions.keys():

                dist = np.linalg.norm(
                    [
                        base_x - placed_positions[close_node_id][0],
                        base_y - placed_positions[close_node_id][1],
                    ]
                )
                if dist < threshold:
                    # Move to the right clockwise slightly
                    angle += shift_angle
                    base_x = radius * np.sin(angle)
                    base_y = radius * np.cos(angle)

            placed_positions[node_id] = (base_x, base_y)

            ax.plot(base_x, base_y, "o", color="lightblue", markersize=10)  # Blue nodes
            text_x = (radius + 0.1) * np.sin(angle)
            text_y = (radius + 0.1) * np.cos(angle)
            ax.text(
                text_x,
                text_y,
                node_id,
                fontsize=12,
                ha="center",
                va="center",
                color="black",
            )

        # Remove axis ticks and labels
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_xticklabels([])
        ax.set_yticklabels([])
        ax.set_title("Pastry Network Visualization")

        # Save the plot
        plt.savefig("Plots/pastry_network_visualization.png")

        plt.show()

    def visualize_topology(self):
        """
        Visualizes the Pastry network by placing nodes as points on a horizontal line [0,1]
        based on the nodes' position attribute (which is a float in [0,1]).
        """
        if not self.nodes:
            print("No nodes in the network to visualize.")
            return

        fig, ax = plt.subplots(figsize=(10, 2))  # Wide aspect ratio for clarity
        ax.set_xlim(0, 1)
        ax.set_ylim(-0.1, 0.1)  # Small height since it's a 1D layout

        # Sort nodes by position for a structured layout
        sorted_nodes = sorted(self.nodes.values(), key=lambda node: node.position)

        # Plot each node at its position on the horizontal line
        for node in sorted_nodes:
            x = node.position
            ax.plot(x, 0, "o", color="lightblue", markersize=10)  # Node as a point
            ax.text(
                x,
                0.025,
                node.node_id,
                fontsize=10,
                ha="center",
                va="center",
                color="black",
            )  # Label above

        # Draw a horizontal line to represent the topology
        ax.plot([0, 1], [0, 0], color="gray", linestyle="--")

        # Remove y-axis ticks and labels since it's a 1D layout
        ax.set_yticks([])
        ax.set_xticks(np.linspace(0, 1, 11))  # Tick marks at [0, 0.1, ..., 1]
        ax.set_xlabel("Node Position in Pastry Network")

        ax.set_title("Pastry Network Topology")

        # Save the plot
        plt.savefig("Plots/pastry_network_topology.png")

        plt.show()
