import pandas as pd

from .helper_functions import *
from .chord_gui import ChordDashboard
from .node import ChordNode


class ChordNetwork:
    bootstrap_node = None

    def __init__(self, main_window=None):
        self.nodes = {}  # Dictionary. Keys are node IDs, values are Node objects
        self.keys = {}
        self.used_ports = []

        self.gui = ChordDashboard(self, main_window)

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

        successor_id, hops = new_node.request_find_successor(
            node_id, ChordNetwork.bootstrap_node, []
        )
        # new_node joins on successor
        new_node.join(self.nodes[successor_id])

    def build(self, predefined_ids):
        """
        Build the Chord network with the specified number of nodes.
        """
        # Node Arrivals
        print("Node Arrivals")
        print("=======================")
        print(f"Adding {len(predefined_ids)} nodes to the network...")
        print("\n" + "-" * 100)
        for node_id in predefined_ids:
            node = ChordNode(self, node_id=node_id)
            print(f"Adding Node: ID = {node.node_id}")
            node.start_server()
            self.node_join(node)
            print(f"\nNode Added: ID = {node.node_id}")
            print("\n" + "-" * 100)
        print("\nAll nodes have successfully joined the network.\n")

        # Insert keys
        # Load dataset
        dataset_path = "Coffee_Reviews_Dataset/simplified_coffee.csv"
        df = pd.read_csv(dataset_path)

        # Keep only the year from the review_date column
        df["review_date"] = pd.to_datetime(df["review_date"], format="%B %Y").dt.year

        # Extract loc_country as keys
        keys = df["loc_country"].apply(hash_key)

        # Extract data points (review_date, rating, 100g_USD)
        points = df[["review_date", "rating", "100g_USD"]].to_numpy()

        # Extract reviews and other details
        reviews = df["review"].to_numpy()
        countries = df["loc_country"].to_numpy()
        names = df["name"].to_numpy()

        print("Key Insertions")
        print("=======================")
        print("\nInserting data into the network...")

        # Insert all entries
        for key, point, review, country, name in zip(keys, points, reviews, countries, names):
            print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
            self.insert_key(key, point, review, country)

        # Show the Chord GUI
        self.gui.show_chord_gui()
        # Run the gui main loop
        self.gui.root.mainloop()

    '''def visualize_network(self, threshold=0.2):
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

        plt.show()'''

    def insert_key(self, key, point, review, country):
        return ChordNetwork.bootstrap_node.insert_key(key, point, review, country)

    def delete_key(self, key):
        return ChordNetwork.bootstrap_node.delete_key(key)

    def update_key(self, key, updated_data, criteria=None):
        return ChordNetwork.bootstrap_node.update_key(key, updated_data, criteria=None)

    def lookup(self, key, lower_bounds, upper_bounds, N):
        return ChordNetwork.bootstrap_node.lookup(key, lower_bounds, upper_bounds, N)
