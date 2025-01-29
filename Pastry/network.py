import matplotlib.pyplot as plt
from matplotlib.widgets import Button
from matplotlib.collections import PathCollection
import numpy as np
import time
import tkinter as tk
from tkinter import simpledialog

from helper_functions import *
from node import PastryNode

evenly_spaced_nodes = 16
positions = positions = np.linspace(0, 1, evenly_spaced_nodes)  # Generates evenly spaced points


class PastryNetwork:
    def __init__(self):
        self.nodes = {}  # Dictionary. Keys are node IDs, values are Node objects
        self.node_ports = {}  # Dictionary. Keys are node IDs, values are ports
        self.used_ports = []
        self.used_positions = list(positions)

        # Initialize plotting
        self.setup_plot()

    def node_join(self, new_node):
        """
        Handles a new node joining the Pastry network.
        """
        # Determine the new node's ID
        new_node_id = new_node.node_id

        # Check if the node ID is already in use
        if new_node_id in self.nodes.keys():
            print(f"Node {new_node_id} already exists in the network.")
            return {"status": "failure", "message": f"Node {new_node_id} already exists."}

        if self.used_positions:
            new_node.position = self.used_positions.pop(0)
        else:
            new_node.position = np.random.uniform(0, 1)  # Fallback

        # Add the node object to the network
        self.nodes[new_node_id] = new_node

        # Add the node's port to the node_ports dictionary
        self.node_ports[new_node_id] = new_node.port

        if len(self.nodes) == 1:
            print("The network is empty. The new node is the first node.")
            self.visualize_network()
            self.visualize_topology()
            return

        # Find the closest node to the new using its position
        closest_node_id, closest_neighborhood_set = self._find_topologically_closest_node(new_node)

        # Initialize the new nodes Neighborhood Set of the new node
        print(f"\nInitializing Neighborhood Set of the new node {new_node_id}...")
        new_node.initialize_neighborhood_set(closest_node_id, closest_neighborhood_set)

        # Forward the join message to the topologically closest node
        join_request = {
            "operation": "NODE_JOIN",
            "joining_node_id": new_node_id,
            "hops": [],  # Initialize an empty hops list
        }
        print(f"\nForwarding JOIN_NETWORK request to the closest node {closest_node_id}...")
        response = new_node.send_request(self.node_ports[closest_node_id], join_request)

        # Extract and print the hop count from the response
        if response and "hops" in response:
            hop_count = len(response["hops"])
            print(f"\nHops during node join for {new_node_id}: {hop_count}")
            print(f"Full hops list: {response['hops']}")
        else:
            print(f"Failed to retrieve hop count for {new_node_id}. Response: {response}")

        # Broadcast the new node's arrival to the network
        print(f"\nBroadcasting the new node's arrival to the network...")
        new_node.transmit_state()

        # Visualize the network after the new node joins
        self.visualize_network()
        self.visualize_topology()

        return response

    def leave(self, leaving_node_id):
        """
        Handles the leave operation for a node in the network.
        """
        print(f"Network: Processing leave request for Node {leaving_node_id}.")

        # Initialize hops tracking
        hops = []

        # Check if the node exists in the network
        if leaving_node_id not in self.nodes:
            print(f"Network: Node {leaving_node_id} does not exist.")
            return {"status": "failure", "message": f"Node {leaving_node_id} not found.", "hops": hops}

        leaving_node = self.nodes[leaving_node_id]
        keys_to_store = []

        with leaving_node.lock:
            # Extract keys from the KDTree of the leaving node
            if not leaving_node.kd_tree or not leaving_node.kd_tree.points.size:
                print(f"Network: Node {leaving_node_id} has no keys to store.")
            else:
                # Extract points, reviews, and countries from the KDTree
                keys_to_store = [
                    {
                        "key": country_key,
                        "position": point,
                        "review": review,
                        "country": country,
                    }
                    for country_key, point, review, country in zip(
                        leaving_node.kd_tree.country_keys,
                        leaving_node.kd_tree.points,
                        leaving_node.kd_tree.reviews,
                        leaving_node.kd_tree.countries,  # Use the original country
                    )
                ]
                print(f"Network: Stored {len(keys_to_store)} keys from Node {leaving_node_id}.")

        # Remove the leaving node from the network
        print(f"Network: Removing Node {leaving_node_id} from the network.")
        del self.nodes[leaving_node_id]
        del self.node_ports[leaving_node_id]

        # Notify remaining nodes about the departure
        print(f"Network: Notifying affected nodes about Node {leaving_node_id}'s departure.")
        available_nodes = list(self.nodes.keys())
        node_positions = {node_id: self.nodes[node_id].position for node_id in self.nodes}

        for node_id in available_nodes:
            leave_request = {
                "operation": "NODE_LEAVE",
                "leaving_node_id": leaving_node_id,
                "available_nodes": available_nodes,
                "node_positions": node_positions,
                "hops": hops,  # Pass the hops list
            }
            response = self.nodes[node_id].send_request(self.node_ports[node_id], leave_request)

            # Update the hops list from the response
            if response and "hops" in response:
                hops = response["hops"]

        # Check if there are any available nodes for reinsertion
        if not available_nodes:
            print("Network: No available nodes to reinsert keys. Keys will not be reinserted.")
            return {"status": "failure", "message": "No nodes available for reinsertion.", "hops": hops}

        # Reinsert stored keys using the network-level insert_key function
        print(f"Network: Reinserting stored keys into the network.")
        reinserted_count = 0
        for key_data in keys_to_store:
            key = key_data["key"]
            position = key_data["position"]
            review = key_data["review"]
            country = key_data["country"]  # Original country

            # Find the node with the minimum subtraction value from the key
            closest_node_id = min(available_nodes, key=lambda node_id: abs(int(node_id, 16) - int(key, 16)))

            try:
                print(f"Network: Redirecting key {key} (Country: {country}) from Node: {node_id}  to Node {closest_node_id}.")
                self.nodes[closest_node_id].insert_key(key, position, review, country)
                reinserted_count += 1
            except Exception as e:
                print(f"Network: Failed to redirect key {key} to Node {closest_node_id}. Error: {e}")

        skipped_count = len(keys_to_store) - reinserted_count
        print(f"Network: Successfully reinserted {reinserted_count} keys. Skipped {skipped_count} keys.")

        print(f"Network: Node {leaving_node_id} has successfully left the network.")
        print(f"The Hopes after leaving are: {hops}")

        # for node_id, node in self.nodes.items():
        #     print(f"Node {node_id}: KD-Tree Keys: {node.kd_tree.country_keys if node.kd_tree else 'None'}")
        #     print(f"Node {node_id}: Routing Table: {node.routing_table}")

        return {"status": "success", "message": f"Node {leaving_node_id} has left the network.", "hops": hops}

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

    def visualize_network(self, threshold=0.3):
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

        self.ax_ring.clear()  # Clear the current figure instead of making a new one
        self.ax_ring.set_xlim(-1.2, 1.2)
        self.ax_ring.set_ylim(-1.2, 1.2)

        # Draw the circular ring
        radius = 1
        circle = plt.Circle((0, 0), radius, color="lightgray", fill=False)
        self.ax_ring.add_patch(circle)

        placed_positions = {}  # Store positions for overlap checking

        # Arrange nodes based on their numerical value
        for node_id in sorted_nodes:
            angle = 2 * np.pi * (int(node_id, 16) / 0xFFFF)
            base_x, base_y = radius * np.sin(angle), radius * np.cos(angle)

            # Check for overlap within the threshold distance
            shift_angle = np.radians(6.5)  # Base shift distance
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

            node_plot = self.ax_ring.scatter(base_x, base_y, color="lightblue", s=100, picker=True)
            node_plot.set_gid(node_id)  # Store the node ID in the plot

            text_offset = 0.05  # 0.1
            text_x = (radius + text_offset) * np.sin(angle)
            text_y = (radius + text_offset) * np.cos(angle)
            ha = "center"
            va = "center"
            if text_x > 0:  # Right half of the circle
                ha = "left"
            else:  # Left half of the circle
                ha = "right"
            self.ax_ring.text(
                text_x,
                text_y,
                node_id,
                fontsize=10,
                ha=ha,
                va=va,
                color="black",
            )

        # Remove axis ticks and labels
        self.ax_ring.set_xticks([])
        self.ax_ring.set_yticks([])
        self.ax_ring.set_title("Pastry Overlay Network Visualization")

        plt.draw()

    def visualize_topology(self):
        """
        Visualizes the Pastry network by placing nodes as points on a horizontal line [0,1]
        based on the nodes' position attribute (which is a float in [0,1]).
        """
        if not self.nodes:
            print("No nodes in the network to visualize.")
            return

        self.ax_topology.clear()
        self.ax_topology.set_xlim(0, 1)
        self.ax_topology.set_ylim(-0.1, 0.1)  # Small height since it's a 1D layout

        # Sort nodes by position for a structured layout
        sorted_nodes = sorted(self.nodes.values(), key=lambda node: node.position)

        # Plot each node at its position on the horizontal line
        for node in sorted_nodes:
            x = node.position

            node_plot = self.ax_topology.scatter(x, 0, color="lightblue", s=100, picker=True)
            node_plot.set_gid(node.node_id)  # Store the node ID in the plot
            # self.ax_topology.plot(x, 0, "o", color="lightblue", markersize=10)  # Node as a point
            self.ax_topology.text(
                x,
                0.025,
                node.node_id,
                fontsize=10,
                ha="center",
                va="center",
                color="black",
            )  # Label above

        # Draw a horizontal line to represent the topology
        self.ax_topology.plot([0, 1], [0, 0], color="gray", linestyle="--")

        # Remove y-axis ticks and labels since it's a 1D layout
        self.ax_topology.set_yticks([])
        self.ax_topology.set_xticks(np.linspace(0, 1, 11))  # Tick marks at [0, 0.1, ..., 1]

        self.ax_topology.set_title("Pastry Network Topology")

        plt.draw()

    def on_node_pick(self, event):
        """Handles a pick event when a node is clicked."""
        artist = event.artist  # Get the picked object

        if isinstance(artist, PathCollection):  # Ensure it's a scatter point
            node_id = artist.get_gid()  # Get stored node ID
            selected_node = self.nodes.get(node_id)

            if selected_node:
                self.update_info_panel(selected_node)

    def update_info_panel(self, node):
        """Print node information in the right panel."""
        self.ax_info.clear()
        self.ax_info.set_xticks([])
        self.ax_info.set_yticks([])
        self.ax_info.set_title("Node Information")

        y_offset = 0.95

        text = node.get_state()  # Get multi-line text

        lines = text.split("\n")  # Split into separate lines
        for line in lines:
            self.ax_info.text(0.02, y_offset, line, fontsize=9, verticalalignment="top", family="monospace")
            y_offset -= 0.03  # Move down for the next line

        plt.draw()

    def setup_plot(self):
        # Initialize figure
        WIDTH = 17
        HEIGHT = 8
        self.fig = plt.figure(figsize=(WIDTH, HEIGHT))
        aspect_ratio = WIDTH / HEIGHT

        # Create buttons
        self.add_button_ax = self.fig.add_axes([0.025, 0.9, 0.1, 0.05])  # Button position on left
        self.add_button = Button(self.add_button_ax, "Node Join")

        # Connect buttons to functions
        self.add_button.on_clicked(self.node_join_gui)

        # Create main visualization (Pastry Ring)
        self.ax_ring = self.fig.add_axes([0.15, 0.3, 0.7 / aspect_ratio, 0.65])  # [x, y, width, height]
        self.ax_ring.set_xlim(-1.2, 1.2)
        self.ax_ring.set_ylim(-1.2, 1.2)
        self.ax_ring.set_xticks([])
        self.ax_ring.set_yticks([])
        self.ax_ring.set_title("Pastry Overlay Network Visualization")

        # Create topology visualization (Bottom)
        self.ax_topology = self.fig.add_axes([0.15, 0.05, 0.75, 0.2])  # Bottom bar for topology
        self.ax_topology.set_xlim(0, 1)
        self.ax_topology.set_ylim(-0.1, 0.1)
        self.ax_topology.set_xticks(np.linspace(0, 1, 11))
        self.ax_topology.set_yticks([])
        self.ax_topology.set_title("Pastry Network Topology")

        # Create node info panel (Right)
        self.ax_info = self.fig.add_axes([0.50, 0.3, 0.4, 0.65])  # Right panel
        self.ax_info.set_xticks([])
        self.ax_info.set_yticks([])
        self.ax_info.set_title("Node Information")

        # Connect the pick event
        self.fig.canvas.mpl_connect("pick_event", self.on_node_pick)

        plt.show(block=False)

    def node_join_gui(self, event=None):
        """
        Handles the GUI event for adding a new node when the 'Node Join' button is clicked.
        """
        """root = tk.Tk()
        root.withdraw()  # Hide the main window
        new_node_id = simpledialog.askstring("Node Join", "Enter the new node ID (4-digit hex):")"""

        def submit(*args):
            nonlocal new_node_id
            new_node_id = entry.get().strip().lower()
            root.destroy()  # Close the window

        root = tk.Tk()
        root.title("Node Join")
        root.geometry("400x150")  # Set a wider window size
        root.resizable(False, False)
        root.lift()
        root.attributes("-topmost", True)  # Keep window on top

        tk.Label(root, text="Enter the new node ID (4-digit hex):", font=("Arial", 12)).pack(pady=10)

        entry = tk.Entry(root, font=("Arial", 12), width=20)
        entry.pack(pady=5)
        root.focus_force()
        entry.focus_set()  # Automatically focus the input field

        tk.Button(root, text="Submit", font=("Arial", 12), command=submit).pack(pady=10)

        # Bind Enter key to submit function
        entry.bind("<Return>", submit)

        new_node_id = None
        root.wait_window(root)

        if not new_node_id:
            print("\nNode join canceled.")
            return

        if len(new_node_id) != 4 or not all(c in "0123456789abcdefABCDEF" for c in new_node_id):
            print("\nInvalid Node ID. Must be a 4-digit hexadecimal value.")
            return

        node = PastryNode(self, node_id=new_node_id)
        print(f"Adding Node: ID = {node.node_id}")
        node.start_server()
        time.sleep(0.1)  # Allow the server to start
        response = self.node_join(node)

        if response and response["status"] == "success":
            print(f"\nNode Added: ID = {node.node_id}, Position = {node.position}")
            print("\n" + "-" * 100)

            # Update visualizations
            self.visualize_network()
            self.visualize_topology()
