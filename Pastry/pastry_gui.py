import matplotlib.pyplot as plt
import numpy as np
import tkinter as tk

from dashboard import Dashboard
from .node import PastryNode


class PastryDashboard(Dashboard):
    def __init__(self, network, main_window):
        super().__init__(network, main_window, "Pastry GUI")
        self.canvas.mpl_connect("pick_event", self.on_node_pick)
        self.setup_dht_specific_components()

    def on_close(self):
        """Shut down Pastry network before closing GUI."""
        print("Shutting down network...")
        for node in self.network.nodes.values():
            node.running = False

        # Stop Tkinter main loop
        self.root.quit()  # Exit the event loop
        self.root.destroy()  # Destroy the GUI window
        self.main_window.deiconify()  # Show the main window again

    def setup_dht_specific_components(self):
        """Add Pastry specific components"""
        # Node leave button
        self.node_leave_button = tk.Button(
            self.control_frame,
            text="Node Leave",
            command=self.node_leave_gui,
            width=15,
            height=2,
            font=("Arial", 14),
        )
        self.node_leave_button.pack(pady=10, padx=10)

        # Node leave unexpected button
        self.node_leave_unexpected_button = tk.Button(
            self.control_frame,
            text="Node Leave Unexpected",
            command=self.node_leave_unexpected_gui,
            width=15,
            height=2,
            font=("Arial", 14),
            wraplength=150,
            justify=tk.CENTER,
        )
        self.node_leave_unexpected_button.pack(pady=10, padx=10)

    def setup_visualization(self):
        """Setup Pastry visualization (ring and topology)"""
        # Topology visualization (Bottom)
        topology_pad = 0.05

        self.topology_x = topology_pad
        self.topology_y = topology_pad
        self.topology_width = (
            1 - self.topology_x - topology_pad
        )  # 100% of the fig width - the padding left and right
        self.topology_height = 1 / 4 - self.topology_y  # 1/4 of the fig height - the padding
        self.ax_topology = self.fig.add_axes(
            [self.topology_x, self.topology_y, self.topology_width, self.topology_height]
        )

        self.ax_topology.set_xlim(0, 1)
        self.ax_topology.set_ylim(-0.1, 0.1)
        self.ax_topology.set_xticks(np.linspace(0, 1, 11))
        self.ax_topology.set_yticks([])
        self.ax_topology.set_title("Pastry Network Topology")

        # Create main visualization (Pastry Ring)
        ring_pad_bottom = 0.03
        ring_pad_top = 0.05
        topology_x_mid = (self.topology_x + (self.topology_x + self.topology_width)) / 2
        topology_x_quarter = (self.topology_x + topology_x_mid) / 2
        topology_x_eighth = (self.topology_x + topology_x_quarter) / 2

        self.ring_x = (self.topology_x + topology_x_eighth) / 2
        ring_x_width = self.ring_x - self.topology_x
        self.ring_y = self.topology_y + self.topology_height + ring_pad_bottom
        self.ring_width = self.topology_width - 2 * ring_x_width
        self.ring_height = 1 - self.topology_height - topology_pad - ring_pad_bottom - ring_pad_top

        self.ax_ring = self.fig.add_axes(
            [self.ring_x, self.ring_y, self.ring_width, self.ring_height]
        )

        self.ax_ring.set_xlim(-1.2, 1.2)
        self.ax_ring.set_ylim(-1.2, 1.2)
        self.ax_ring.set_xticks([])
        self.ax_ring.set_yticks([])
        self.ax_ring.set_title("Pastry Overlay Network Visualization")

    def show_visualization(self):
        self.visualize_network()
        self.visualize_topology()

    def clear_visualization(self):
        # Clear the ring
        self.ax_ring.clear()
        self.ax_ring.set_xticks([])
        self.ax_ring.set_yticks([])

        # Clear the topology
        self.ax_topology.clear()
        self.ax_topology.set_xticks([])
        self.ax_topology.set_yticks([])
        self.ax_topology.spines["top"].set_visible(False)
        self.ax_topology.spines["bottom"].set_visible(False)
        self.ax_topology.spines["left"].set_visible(False)
        self.ax_topology.spines["right"].set_visible(False)

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

        self.ax_ring.clear()
        self.ax_ring.set_title("Pastry Overlay Network Visualization")
        self.ax_ring.spines["top"].set_visible(False)
        self.ax_ring.spines["bottom"].set_visible(False)
        self.ax_ring.spines["left"].set_visible(False)
        self.ax_ring.spines["right"].set_visible(False)

        self.ax_ring.set_xticks([])  # Remove x-axis ticks
        self.ax_ring.set_yticks([])  # Remove y-axis ticks

        if not self.network.nodes:
            return

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

            text_offset = 0.05
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

        self.canvas.draw()

    def visualize_topology(self):
        """
        Visualizes the Pastry network by placing nodes as points on a horizontal line [0,1]
        based on the nodes' position attribute (which is a float in [0,1]).
        """
        if not self.network.nodes:
            print("No nodes in the network to visualize.")
            return
        # Draw a horizontal line to represent the topology
        self.ax_topology.clear()
        self.ax_topology.plot([0, 1], [0, 0], color="gray", linestyle="--")
        self.ax_topology.set_xlim(0, 1)
        self.ax_topology.set_ylim(-0.1, 0.1)  # Small height since it's a 1D layout
        self.ax_topology.set_xticks(np.linspace(0, 1, 11))
        self.ax_topology.set_yticks([])
        self.ax_topology.spines["top"].set_visible(True)
        self.ax_topology.spines["bottom"].set_visible(True)
        self.ax_topology.spines["left"].set_visible(True)
        self.ax_topology.spines["right"].set_visible(True)
        self.ax_topology.set_title("Pastry Network Topology")

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

        self.canvas.draw()

    def node_join_gui(self):
        def submit(event=None):  # Accept event argument for key binding
            nonlocal new_node_id
            new_node_id = entry.get().strip().lower()
            join_window.destroy()

        join_window = tk.Toplevel(self.root)
        join_window.title("Node Join")
        join_window.geometry("300x150")

        tk.Label(join_window, text="Enter 4-digit hex ID:", font=("Arial", 14)).pack(pady=10)

        entry = tk.Entry(join_window, width=20)
        entry.pack(pady=2)
        entry.focus_set()  # Set focus to the entry field

        # Bind Enter key to submit function
        entry.bind("<Return>", submit)

        tk.Button(join_window, text="Submit", command=submit, font=("Arial", 12)).pack(pady=10)

        new_node_id = None

        join_window.grab_set()
        self.root.wait_window(join_window)

        if not new_node_id:
            print("\nNode join canceled.")
            return

        if len(new_node_id) != 4 or not all(c in "0123456789abcdefABCDEF" for c in new_node_id):
            print("Invalid Node ID.")
            return

        node = PastryNode(self.network, node_id=new_node_id)
        print(f"\nAdding new node with ID: {node.node_id} to the network.")
        node.start_server()
        self.network.node_join(node)
        self.show_dht_gui()

    def node_leave_gui(self):
        if self.selected_node is None:
            print("No node selected.")
            return

        print("Node is leaving gracefully...")

        leaving_node_id = self.selected_node.node_id

        leave_response = self.network.leave(leaving_node_id)
        if leave_response and "hops" in leave_response:
            print(f"Hops during NODE_LEAVE for {leaving_node_id}: {len(leave_response['hops'])}")
        else:
            print(f"Failed to retrieve hops for NODE_LEAVE {leaving_node_id}.")

        self.show_dht_gui()

    def node_leave_unexpected_gui(self):
        if self.selected_node is None:
            print("No node selected.")
            return

        print("Node left unexpectedly.")
        leaving_node_id = self.selected_node.node_id

        self.network.leave_unexpected(leaving_node_id)

        self.show_dht_gui()
