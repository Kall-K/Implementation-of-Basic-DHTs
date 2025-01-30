import matplotlib.pyplot as plt
from matplotlib.widgets import Button
from matplotlib.collections import PathCollection
import numpy as np
import time
import tkinter as tk

from node import PastryNode


class GUI:
    def __init__(self, network):
        self.network = network

        self.setup_plot()

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

    def visualize_network(self, threshold=0.3):
        """
        Visualizes the Pastry network by placing nodes on a circular ring
        based on their 4-digit hex ID. Lower values are at the top (12 o'clock),
        and values increase clockwise.

        Nodes that are too close together will be moved slightly.
        """
        if not self.network.nodes:
            print("No nodes in the network to visualize.")
            return

        # Convert node IDs from hex to integers for sorting
        sorted_nodes = sorted(self.network.nodes.keys(), key=lambda x: int(x, 16))

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
        if not self.network.nodes:
            print("No nodes in the network to visualize.")
            return

        self.ax_topology.clear()
        self.ax_topology.set_xlim(0, 1)
        self.ax_topology.set_ylim(-0.1, 0.1)  # Small height since it's a 1D layout

        # Sort nodes by position for a structured layout
        sorted_nodes = sorted(self.network.nodes.values(), key=lambda node: node.position)

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
            selected_node = self.network.nodes.get(node_id)

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

        node = PastryNode(self.network, node_id=new_node_id)
        print(f"Adding Node: ID = {node.node_id}")
        node.start_server()
        time.sleep(0.1)  # Allow the server to start
        response = self.network.node_join(node)

        if response and response["status"] == "success":
            print(f"\nNode Added: ID = {node.node_id}, Position = {node.position}")
            print("\n" + "-" * 100)

            # Update visualizations
            self.visualize_network()
            self.visualize_topology()
