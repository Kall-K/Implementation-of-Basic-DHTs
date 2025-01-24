import matplotlib.pyplot as plt
import numpy as np

from helper_functions import *
from constants import N

positions = positions = np.linspace(0, 1, N, endpoint=False)  # Generates evenly spaced points


class PastryNetwork:
    def __init__(self):
        self.nodes = {}  # Dictionary. Keys are node IDs, values are Node objects
        self.node_ports = {}  # Dictionary. Keys are node IDs, values are ports
        self.used_ports = []
        self.used_positions = list(positions)

    def node_join(self, new_node):
        """
        Handles a new node joining the Pastry network.
        """
        if self.used_positions:
            new_node.position = self.used_positions.pop(0)
        else:
            new_node.position = np.random.uniform(0, 1)  # Fallback

        # Determine the new node's ID
        new_node_id = new_node.node_id

        # Add the node object to the network
        self.nodes[new_node_id] = new_node

        # Add the node's port to the node_ports dictionary
        self.node_ports[new_node_id] = new_node.port

        if len(self.nodes) == 1:
            print("The network is empty. The new node is the first node.")
            return

        # Find the closest node to the new using its position
        closest_node_id, closest_neighborhood_set = self._find_topologically_closest_node(new_node)

        # Initialize the new nodes Neighborhood Set of the new node
        print(f"\nInitializing Neighborhood Set of the new node {new_node_id}...")
        new_node.initialize_neighborhood_set(closest_node_id, closest_neighborhood_set)

        # Forward the join message to the topologically closest node
        join_request = {
            "operation": "JOIN_NETWORK",
            "joining_node_id": new_node_id,
            "hops": [],
        }
        print(f"\nForwarding JOIN_NETWORK request to the closest node {closest_node_id}...")
        response = new_node.send_request(self.node_ports[closest_node_id], join_request)
        print(response)

        # Broadcast the new node's arrival to the network
        print(f"\nBroadcasting the new node's arrival to the network...")
        new_node.transmit_state()

    def _find_topologically_closest_node(self, new_node):
        """
        Find the topologically closest node in the network to the new node.
        """
        closest_node_id = None
        closest_neighborhood_set = None
        min_distance = float("inf")
        for existing_node_id in self.node_ports.keys():
            # Skip the new node
            if existing_node_id == new_node.node_id:
                continue

            dist_request = {
                "operation": "DISTANCE",
                "node_position": new_node.position,
                "hops": [],
            }
            response = new_node.send_request(self.node_ports[existing_node_id], dist_request)

            distance = response["distance"]
            neighborhood_set = response["neighborhood_set"]
            if distance < min_distance:
                closest_node_id = existing_node_id
                closest_neighborhood_set = neighborhood_set
                min_distance = distance
        return closest_node_id, closest_neighborhood_set

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
