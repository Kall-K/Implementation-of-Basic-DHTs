from helper_functions import *
import matplotlib.pyplot as plt
import numpy as np

class ChordNetwork:
    def __init__(self):
        self.nodes = {}  # Dictionary. Keys are node IDs, values are Node objects
        self.used_ports = []

    def node_join(self, new_node):
        """
        Handles a new node joining the Chord network.
        """
        # Determine the node ID
        node_id = new_node.node_id

        # # Add the node's port to the node_ports dictionary
        # self.node_ports[new_node.node_id] = new_node.port

        if len(self.nodes) == 1 or all(not node.running for node in self.nodes.values()):
            print("The network is empty. This node is the first node.")
            self.nodes[node_id] = new_node
            return
        
        # Find the running node to insert the new node
        running_node = 0
        for i in self.nodes.keys():
            if self.nodes[i].running:
                running_node = self.nodes[i]
                break

        self.nodes[node_id] = new_node
        
        successor_id, hops = new_node.request_find_successor(node_id, running_node, [])
        # new_node joins on successor
        new_node.join(self.nodes[successor_id])


    def visualize_network(self, threshold=0.2):
        """
        Visualizes the Chord network by placing nodes on a circular ring
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
        ax.set_title("Chord Network Visualization")

        # Save the plot
        plt.savefig("../Chord/Plots/chord_network_visualization.png")

        plt.show()

    def insert_key(self, key, point, review, country):  
        for node_id in self.nodes.keys():
            if self.nodes[node_id].running:
                return self.nodes[node_id].insert_key(key, point, review, country)

    def delete_key(self, key):
        for node_id in self.nodes.keys():
            if self.nodes[node_id]:
                return self.nodes[node_id].delete_key(key)  
    
    def update_key(self, key, updated_data, criteria=None):
        for node_id in self.nodes.keys():
            if self.nodes[node_id]:
                return self.nodes[node_id].update_key(key, updated_data, criteria)
        
    def lookup(self, key, lower_bounds, upper_bounds, N):
        for node_id in self.nodes.keys():
            if self.nodes[node_id]:
                return self.nodes[node_id].lookup(key, lower_bounds, upper_bounds, N)
