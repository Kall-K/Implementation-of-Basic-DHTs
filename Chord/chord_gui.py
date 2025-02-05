import matplotlib.pyplot as plt
import numpy as np
import tkinter as tk

from dashboard import Dashboard
from .node import ChordNode


class ChordDashboard(Dashboard):
    def __init__(self, network, main_window):
        super().__init__(network, main_window, "Chord GUI")
        self.canvas.mpl_connect("pick_event", self.on_node_pick)
        self.setup_dht_specific_components()

    def on_close(self):
        """Shut down Chord network before closing GUI."""
        print("Shutting down network...")
        for node in self.network.nodes.values():
            node.leave()

        # Stop Tkinter main loop
        self.root.quit()  # Exit the event loop
        self.root.destroy()  # Destroy the GUI window
        self.main_window.deiconify()  # Show the main window again

    def setup_dht_specific_components(self):
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
        """Setup Chord visualization (ring only)"""
        # Create main visualization (Chord Ring)
        ring_pad_side = 0.03

        self.ring_x = ring_pad_side
        self.ring_y = 0
        self.ring_width = 1 - 2 * ring_pad_side
        self.ring_height = self.ring_width

        self.ax_ring = self.fig.add_axes(
            [self.ring_x, self.ring_y, self.ring_width, self.ring_height]
        )

        self.ax_ring.set_xlim(-1.2, 1.2)
        self.ax_ring.set_ylim(-1.2, 1.2)
        self.ax_ring.set_xticks([])
        self.ax_ring.set_yticks([])
        self.ax_ring.set_title("Chord Overlay Network Visualization")

    def show_visualization(self):
        self.visualize_network()

    def clear_visualization(self):
        # Clear the ring
        self.ax_ring.clear()
        self.ax_ring.set_xticks([])
        self.ax_ring.set_yticks([])

    def visualize_network(self, threshold=0.3):
        """
        Visualizes the Chord network by placing nodes on a circular ring
        based on their 4-digit hex ID. Lower values are at the top (12 o'clock),
        and values increase clockwise.

        Nodes that are too close together will be moved slightly.
        """
        if not self.network.nodes:
            print("No nodes in the network to visualize.")
            return

        # Filter nodes that have the 'running' attribute set to True
        filtered_nodes = [node_id for node_id, node in self.network.nodes.items() if node.running]

        # Sort the filtered nodes by their hex ID
        sorted_nodes = sorted(filtered_nodes, key=lambda x: int(x, 16))

        self.ax_ring.clear()
        self.ax_ring.set_title("Chord Overlay Network Visualization")
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

        node = ChordNode(self.network, node_id=new_node_id)
        print(f"\nAdding new node with ID: {node.node_id} to the network.")
        node.start_server()
        self.network.node_join(node)
        self.show_dht_gui()

    def node_leave_unexpected_gui(self):
        if self.selected_node is None:
            print("No node selected.")
            return

        print("Node left unexpectedly.")
        leaving_node = self.network.nodes[self.selected_node.node_id]

        leaving_node.leave()

        self.show_dht_gui()
