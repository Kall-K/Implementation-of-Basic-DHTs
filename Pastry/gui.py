import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import numpy as np
import tkinter as tk
from tkinter import scrolledtext
from matplotlib.collections import PathCollection

from node import PastryNode


WIDTH = 1680
HEIGHT = 720


class GUI:
    def __init__(self, network):
        self.network = network
        self.selected_node = None
        self.root = tk.Tk()
        self.root.title("Pastry GUI")
        self.root.geometry(f"{WIDTH}x{HEIGHT}")

        # Ensure cleanup on close
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        # Setup main layout
        self.setup_widgets()

    def on_close(self):
        """Shut down Pastry network before closing GUI."""
        print("Shutting down network...")
        for node in self.network.nodes.values():
            node.running = False

        # Stop Tkinter main loop
        self.root.quit()  # Exit the event loop
        self.root.destroy()  # Destroy the GUI window

    def setup_widgets(self):
        # Left frame for control buttons
        control_width = WIDTH // 8
        control_frame = tk.Frame(self.root, width=control_width, height=HEIGHT, bg="lightgray")
        control_frame.pack(side=tk.LEFT, fill=tk.Y)
        control_frame.pack_propagate(False)  # Prevents resizing

        # Show Pastry button
        self.show_pastry_button = tk.Button(
            control_frame,
            text="Show Pastry",
            command=self.show_pastry_gui,
            width=15,
            height=2,
            font=("Arial", 14),
        )
        self.show_pastry_button.pack(pady=10, padx=10)

        # Show KD Tree button
        self.show_kd_tree_button = tk.Button(
            control_frame,
            text="Show KD Tree",
            command=self.show_kd_tree_gui,
            width=15,
            height=2,
            font=("Arial", 14),
        )
        self.show_kd_tree_button.pack(pady=10, padx=10)

        # Node join button
        self.node_join_button = tk.Button(
            control_frame,
            text="Node Join",
            command=self.node_join_gui,
            width=15,
            height=2,
            font=("Arial", 14),
        )
        self.node_join_button.pack(pady=10, padx=10)

        # Center frame for visualizations
        viz_width = HEIGHT
        self.viz_frame = tk.Frame(self.root, width=viz_width, height=HEIGHT)
        self.viz_frame.pack(side=tk.LEFT, fill=tk.Y)

        fig_width = viz_width / 100
        fig_height = HEIGHT / 100
        self.fig = plt.figure(figsize=(fig_width, fig_height))

        # Create topology visualization (Bottom)
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

        self.canvas = FigureCanvasTkAgg(self.fig, master=self.viz_frame)
        self.canvas.get_tk_widget().pack(fill=tk.Y, expand=True)
        self.pastry_node_pick_event_id = self.canvas.mpl_connect("pick_event", self.on_node_pick)

        # Right frame for node info
        info_frame = tk.Frame(self.root)
        info_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        info_frame.pack_propagate(False)  # Prevents resizing

        tk.Label(info_frame, text="Node Information", font=("Arial", 14)).pack()
        self.info_text = scrolledtext.ScrolledText(info_frame, wrap=tk.WORD, width=30, height=35)
        self.info_text.pack(expand=True, fill=tk.BOTH)

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

    def on_node_pick(self, event):
        if isinstance(event.artist, PathCollection):
            node_id = event.artist.get_gid()
            selected_node = self.network.nodes.get(node_id)
            if selected_node:
                self.selected_node = selected_node
                self.update_info_panel(selected_node)

    def update_info_panel(self, node):
        """Print node information in the right panel."""
        self.info_text.config(state=tk.NORMAL)
        self.info_text.delete(1.0, tk.END)

        # Insert the formatted node state with monospace font
        self.info_text.insert(tk.END, node.get_state())

        # Ensure text uses a fixed-width font for proper alignment
        self.info_text.config(font=("Courier", 11), state=tk.DISABLED)

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
        node.start_server()
        self.network.node_join(node)
        self.visualize_network()
        self.visualize_topology()

    def show_pastry_gui(self):
        """Displays the Pastry ring and topology."""
        # Remove the temporary KD-Tree plot if it exists
        if hasattr(self, "ax_kd_tree"):
            self.ax_kd_tree.remove()
            del self.ax_kd_tree

        # Disconnect the KD-Tree pick event if active
        if hasattr(self, "kd_tree_pick_event_id"):
            self.canvas.mpl_disconnect(self.kd_tree_pick_event_id)
            del self.kd_tree_pick_event_id  # Remove reference

        # Clear all widgets from the root window
        for widget in self.root.winfo_children():
            widget.destroy()

        # Reinitialize the GUI
        self.setup_widgets()

        # Redraw both visualizations
        self.visualize_network()
        self.visualize_topology()

    def show_kd_tree_gui(self):
        """Displays the KD Tree of the selected node if available."""
        if self.selected_node is None:
            print("No node selected.")
            return

        # Check if KD Tree already exists and is displayed
        if hasattr(self, "ax_kd_tree"):
            print("KD Tree is already displayed.")
            return

        if hasattr(self.selected_node, "kd_tree") and self.selected_node.kd_tree:
            # Open a dialog box to let the user select a unique country
            def on_select(event=None):
                nonlocal selected_country
                selected_country = country_var.get()
                if selected_country:
                    selection_window.unbind("<Return>")
                    selection_window.destroy()

            selection_window = tk.Toplevel(self.root)
            selection_window.title("Select Country")
            selection_window.geometry("300x200")

            tk.Label(selection_window, text="Select a country:", font=("Arial", 14)).pack(pady=10)

            country_var = tk.StringVar(selection_window)

            unique_countries, counts = np.unique(
                self.selected_node.kd_tree.countries, return_counts=True
            )

            dropdown = tk.OptionMenu(selection_window, country_var, *unique_countries)
            dropdown.pack(pady=5)

            # Add a button to confirm selection
            tk.Button(selection_window, text="OK", command=on_select, font=("Arial", 12)).pack(
                pady=10
            )

            # Bind Enter key to select action
            selection_window.bind("<Return>", on_select)

            selected_country = None

            selection_window.grab_set()
            self.root.wait_window(selection_window)

            if not selected_country:
                print("Country selection canceled.")
                return

            print(f"Node {self.selected_node.node_id}: Visualizing KD Tree for {selected_country}.")

            # TODO: Na kanw visualize to kd tree mono gia to selected country
            # prepei na vrw ta indices gia to selected country
            # kai na perasw stin visualize ta points, reviews se auta ta indices

            # Clear the ring plot
            self.ax_ring.clear()
            self.ax_ring.set_xticks([])
            self.ax_ring.set_yticks([])

            # Clear the topology plot
            self.ax_topology.clear()
            self.ax_topology.set_xticks([])
            self.ax_topology.set_yticks([])
            self.ax_topology.spines["top"].set_visible(False)
            self.ax_topology.spines["bottom"].set_visible(False)
            self.ax_topology.spines["left"].set_visible(False)
            self.ax_topology.spines["right"].set_visible(False)

            self.ax_kd_tree = self.fig.add_subplot(111, projection="3d")

            # Disconnect Pastry pick event
            self.canvas.mpl_disconnect(self.pastry_node_pick_event_id)

            # Connect KD-Tree pick event and store ID
            self.kd_tree_pick_event_id = self.canvas.mpl_connect(
                "pick_event", self.selected_node.kd_tree.on_pick
            )

            # Visualize the KD Tree
            self.selected_node.kd_tree.visualize(ax=self.ax_kd_tree, canvas=self.canvas)
        else:
            print(f"Node {self.selected_node.node_id} does not have a KD Tree.")
