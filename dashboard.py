# (Common Base Class for the Chord and Pastry dashboards)
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import scrolledtext
from matplotlib.collections import PathCollection
from abc import ABC, abstractmethod

from Pastry.helper_functions import hash_key

WIDTH = 1720
HEIGHT = 750


class Dashboard(ABC):
    def __init__(self, network, main_window, title):
        self.network = network
        self.main_window = main_window
        self.selected_node = None
        self.root = tk.Tk()
        self.root.title(title)
        self.root.geometry(f"{WIDTH}x{HEIGHT}")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.setup_widgets()

    def on_close(self):
        for node in self.network.nodes.values():
            node.leave()

        self.root.quit()
        self.root.destroy()
        self.main_window.deiconify()

    # Common GUI components and basic operations
    def setup_widgets(self):
        # Control frame (left panel)
        control_width = WIDTH // 8
        self.control_frame = tk.Frame(self.root, width=control_width, height=HEIGHT, bg="lightgray")
        self.control_frame.pack(side=tk.LEFT, fill=tk.Y)
        self.control_frame.pack_propagate(False)

        # Visualization frame (center)
        viz_width = HEIGHT
        self.viz_frame = tk.Frame(self.root, width=viz_width, height=HEIGHT)
        self.viz_frame.pack(side=tk.LEFT, fill=tk.Y)

        # Info frame (right panel)
        self.info_frame = tk.Frame(self.root)
        self.info_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        self.info_frame.grid_rowconfigure(0, weight=3)
        self.info_frame.grid_rowconfigure(1, weight=1)
        self.info_frame.grid_columnconfigure(0, weight=1)

        # Common buttons
        buttons = [
            ("Show DHT", self.show_dht_gui),
            ("Show KD Tree", self.show_kd_tree_gui),
            ("Node Join", self.node_join_gui),
            ("Insert Key", self.insert_key_gui),
            ("Update Key", self.update_key_gui),
            ("Delete Key", self.delete_key_gui),
            ("Lookup Key", self.lookup_key_gui),
        ]

        for text, command in buttons:
            btn = tk.Button(
                self.control_frame,
                text=text,
                command=command,
                width=15,
                height=2,
                font=("Arial", 14),
                wraplength=150,
            )
            btn.pack(pady=10, padx=10)

        # Visualization setup
        self.fig = plt.figure(figsize=(viz_width / 100, HEIGHT / 100))
        self.setup_visualization()
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.viz_frame)
        self.canvas.get_tk_widget().pack(fill=tk.Y, expand=True)
        self.node_pick_event_id = self.canvas.mpl_connect("pick_event", self.on_node_pick)

        # Info panel
        self.node_info_label = tk.Label(
            self.info_frame, text="Node Information", font=("Arial", 14)
        )
        self.node_info_label.grid(row=0, column=0, sticky="n", padx=5, pady=5)
        self.info_text = scrolledtext.ScrolledText(
            self.info_frame, wrap=tk.WORD, width=30, height=50
        )
        self.info_text.grid(row=0, column=0, sticky="nsew", padx=5, pady=(35, 0))

    @abstractmethod
    def setup_dht_specific_components(self):
        pass

    @abstractmethod
    def setup_visualization(self):
        pass

    @abstractmethod
    def show_visualization(self):
        pass

    @abstractmethod
    def clear_visualization(self):
        pass

    # Common node pick handler
    def on_node_pick(self, event):
        if isinstance(event.artist, PathCollection):
            node_id = event.artist.get_gid()
            selected_node = self.network.nodes.get(node_id)
            if selected_node:
                self.selected_node = selected_node
                self.update_info_panel(selected_node)

    # Common update info panel method
    def update_info_panel(self, node):
        self.info_text.config(state=tk.NORMAL)
        self.info_text.delete(1.0, tk.END)
        self.info_text.insert(tk.END, node.get_state())
        self.info_text.config(font=("Courier", 11), state=tk.DISABLED)

    # Common select country window
    def select_country_window(self):
        # Get unique countries from the KD Tree of the selected node
        unique_country_keys, unique_countries = self.selected_node.kd_tree.get_unique_country_keys()

        if not unique_countries:
            print("No countries available.")
            return

        # Create a window to select a country
        def on_select(event=None):
            nonlocal selected_country, selected_country_key
            selected_country = country_var.get()
            if selected_country:
                selected_country_key = unique_country_keys[unique_countries.index(selected_country)]
                selection_window.destroy()

        selection_window = tk.Toplevel(self.root)
        selection_window.title("Select Country")
        selection_window.geometry("300x200")

        tk.Label(selection_window, text="Select a country:", font=("Arial", 14)).pack(pady=10)

        country_var = tk.StringVar(selection_window)
        country_var.set(unique_countries[0])  # First country as default

        dropdown = tk.OptionMenu(selection_window, country_var, *unique_countries)
        dropdown.config(font=("Arial", 12))
        dropdown["menu"].config(font=("Arial", 12))
        dropdown.pack(pady=5)

        # Add a Submit button for the selection window
        submit_button = tk.Button(
            selection_window, text="OK", command=on_select, font=("Arial", 12)
        )
        submit_button.pack(pady=10)

        # Bind the Return key to on_select
        selection_window.bind("<Return>", lambda event: on_select())

        selected_country = None
        selected_country_key = None

        selection_window.grab_set()
        self.root.wait_window(selection_window)

        return selected_country, selected_country_key

    # Common show dht method
    def show_dht_gui(self):
        """Displays the DHT Network."""

        # Remove the temporary KD-Tree plot if it exists
        if hasattr(self, "ax_kd_tree"):
            self.ax_kd_tree.remove()
            del self.ax_kd_tree

        # Disconnect the KD-Tree pick event if active
        if hasattr(self, "kd_tree_pick_event_id"):
            self.canvas.mpl_disconnect(self.kd_tree_pick_event_id)
            del self.kd_tree_pick_event_id  # Remove reference

        if hasattr(self, "review_text"):
            self.review_text.destroy()
            del self.review_text

        # Clear all widgets from the root window
        for widget in self.root.winfo_children():
            widget.destroy()

        # Reinitialize the GUI
        self.setup_widgets()
        self.setup_dht_specific_components()

        # Redraw the Visualization
        self.show_visualization()

        # Update the Info Panel
        if self.selected_node and self.selected_node.running:
            self.update_info_panel(self.selected_node)
        else:
            # Clear the info panel text
            self.info_text.config(state=tk.NORMAL)
            self.info_text.delete(1.0, tk.END)
            self.info_text.config(state=tk.DISABLED)

    # Common kd tree visualization method
    def show_kd_tree_gui(
        self,
        selected_country=None,
        selected_country_key=None,
        points=None,
        reviews=None,
        title=None,
    ):
        """Displays the KD Tree of the selected node if available."""
        if self.selected_node is None:
            print("No node selected.")
            return

        if (
            hasattr(self.selected_node, "kd_tree")
            and self.selected_node.kd_tree
            and self.selected_node.kd_tree.points.size
        ):

            # If the selected_country is not provided promt the user to select it
            if selected_country is None or selected_country_key is None:

                # Prompt the user to select a country
                selected_country, selected_country_key = self.select_country_window()

                if not selected_country:
                    print("Country selection canceled.")
                    return

            # Clear the previous KD-Tree plot if it exists
            if hasattr(self, "ax_kd_tree") and hasattr(self, "kd_tree_pick_event_id"):
                self.ax_kd_tree.set_title("")
                self.ax_kd_tree.clear()
                self.ax_kd_tree.set_xticks([])
                self.ax_kd_tree.set_yticks([])
                self.ax_kd_tree.set_zticks([])
                self.canvas.mpl_disconnect(self.kd_tree_pick_event_id)
                del self.kd_tree_pick_event_id  # Remove reference

            if points is None or reviews is None:
                # Get points and reviews for the selected country if they are not provided
                points, reviews = self.network.nodes[self.selected_node.node_id].kd_tree.get_points(
                    selected_country_key
                )

            # Clear the network visualization
            self.clear_visualization()

            # Create a scrollable review panel below the node information panel
            if not hasattr(self, "review_text"):
                # Decrease node info panel height
                self.info_text.config(height=25)

                # Setup review panel
                review_label = tk.Label(
                    self.info_frame, text="Review for selected point", font=("Arial", 14)
                )
                review_label.grid(row=1, column=0, sticky="n", padx=5, pady=5)
                self.review_text = scrolledtext.ScrolledText(
                    self.info_frame, wrap=tk.WORD, width=30, height=5
                )
                self.review_text.grid(row=1, column=0, sticky="nsew", padx=5, pady=(35, 0))

            else:
                # If the review panel already exists, clear it
                self.review_text.config(state=tk.NORMAL)
                self.review_text.delete(1.0, tk.END)
                self.review_text.config(state=tk.DISABLED)

            self.ax_kd_tree = self.fig.add_subplot(111, projection="3d")

            # Disconnect Chord pick event
            self.canvas.mpl_disconnect(self.node_pick_event_id)

            # Connect KD-Tree pick event and store ID
            self.kd_tree_pick_event_id = self.canvas.mpl_connect(
                "pick_event",
                lambda event: self.selected_node.kd_tree.on_pick(
                    event, points, reviews, self.review_text
                ),
            )

            # Visualize the KD Tree
            self.selected_node.kd_tree.visualize(
                self.ax_kd_tree,
                self.canvas,
                points,
                reviews,
                selected_country_key,
                selected_country,
                title,
            )

        else:
            print(f"Node {self.selected_node.node_id} does not have a KD Tree.")

    @abstractmethod
    def node_join_gui(self):
        pass

    # Common DHT Operations (Insert, Update, Delete, Lookup)
    def insert_key_gui(self):
        """Insert a new key into the network."""
        if self.selected_node is None:
            print("No node selected.")
            return

        # Create a new window for inserting a new coffee shop key
        insert_window = tk.Toplevel(self.root)
        insert_window.title("Insert New Coffee Shop Review")
        # Increase the height slightly to accommodate the review field
        insert_window.geometry("450x350")

        # Create labels and entry fields using grid layout
        tk.Label(insert_window, text="Name:", font=("Arial", 12)).grid(
            row=0, column=0, padx=10, pady=5, sticky="e"
        )
        name_entry = tk.Entry(insert_window, font=("Arial", 12))
        name_entry.grid(row=0, column=1, padx=10, pady=5)

        tk.Label(insert_window, text="Country:", font=("Arial", 12)).grid(
            row=1, column=0, padx=10, pady=5, sticky="e"
        )
        country_entry = tk.Entry(insert_window, font=("Arial", 12))
        country_entry.grid(row=1, column=1, padx=10, pady=5)

        tk.Label(insert_window, text="Year:", font=("Arial", 12)).grid(
            row=2, column=0, padx=10, pady=5, sticky="e"
        )
        year_entry = tk.Entry(insert_window, font=("Arial", 12))
        year_entry.grid(row=2, column=1, padx=10, pady=5)

        tk.Label(insert_window, text="Rating:", font=("Arial", 12)).grid(
            row=3, column=0, padx=10, pady=5, sticky="e"
        )
        rating_entry = tk.Entry(insert_window, font=("Arial", 12))
        rating_entry.grid(row=3, column=1, padx=10, pady=5)

        tk.Label(insert_window, text="Price (100g USD):", font=("Arial", 12)).grid(
            row=4, column=0, padx=10, pady=5, sticky="e"
        )
        price_entry = tk.Entry(insert_window, font=("Arial", 12))
        price_entry.grid(row=4, column=1, padx=10, pady=5)

        tk.Label(insert_window, text="Review:", font=("Arial", 12)).grid(
            row=5, column=0, padx=10, pady=5, sticky="ne"
        )
        # Use a Text widget for multi-line review input
        review_text = tk.Text(insert_window, font=("Arial", 12), width=30, height=4)
        review_text.grid(row=5, column=1, padx=10, pady=5)

        def submit():
            name = name_entry.get().strip()
            country = country_entry.get().strip()
            year_str = year_entry.get().strip()
            rating_str = rating_entry.get().strip()
            price_str = price_entry.get().strip()
            review = review_text.get("1.0", tk.END).strip()

            # Validate that all fields are filled
            if not (name and country and year_str and rating_str and price_str and review):
                print("All fields are required.")
                return

            try:
                year = float(year_str)
            except ValueError:
                print("Year must be a number.")
                return

            try:
                rating = float(rating_str)
            except ValueError:
                print("Rating must be a number.")
                return

            try:
                price = float(price_str)
            except ValueError:
                print("Price must be a number.")
                return

            print("\nInserting a new Coffee Shop Review:")
            print(
                f"Name: {name}, Country: {country}, Year: {year}, Rating: {rating}, Price: {price}, Review: {review}"
            )

            # Generate a key for the country (adjust this if needed)
            key = hash_key(country)
            point = [year, rating, price]
            # Insert the new key into the selected node
            self.selected_node.insert_key(key, point, review, country)

            # Close the insert window and update visualizations
            insert_window.destroy()
            self.show_dht_gui()
            self.update_info_panel(self.selected_node)

        submit_button = tk.Button(insert_window, text="Submit", command=submit, font=("Arial", 12))
        submit_button.grid(row=6, column=0, columnspan=2, pady=10)
        insert_window.bind("<Return>", lambda event: submit())

    def update_key_gui(self):
        """Update a key in the network."""
        if self.selected_node is None:
            print("No node selected.")
            return

        # Check if the selected node has a KD-Tree
        if not hasattr(self.selected_node, "kd_tree") or not self.selected_node.kd_tree:
            print(f"Node {self.selected_node.node_id} does not have a KD Tree.")
            return

        # Prompt the user to select a country
        selected_country, selected_country_key = self.select_country_window()

        if not selected_country:
            print("Country selection canceled.")
            return

        # Create a window to input updated data and criteria
        def submit():
            # Collect updated data from the GUI
            updated_year = year_entry.get()
            updated_rating = rating_entry.get()
            updated_price = price_entry.get()
            updated_review = review_text.get("1.0", tk.END).strip()

            # Check if all fields are provided by the user
            if updated_year and updated_rating and updated_price and updated_review:
                # Use the "point" field in update_fields
                updated_data = {
                    "point": [int(updated_year), float(updated_rating), float(updated_price)],
                    "review": updated_review,
                }
            elif not (updated_year or updated_rating or updated_price or updated_review):
                print("At least one field must be updated.")
                return
            else:
                # Use the "attributes" field in update_fields
                updated_data = {
                    "attributes": {
                        "review_date": int(updated_year) if updated_year else None,
                        "rating": float(updated_rating) if updated_rating else None,
                        "price": float(updated_price) if updated_price else None,
                    },
                    "review": updated_review if updated_review else None,
                }
                # Remove None values from updated_data[attributes]
                updated_data["attributes"] = {
                    k: v for k, v in updated_data["attributes"].items() if v is not None
                }
                # Remove updated_data[review] if it is None
                if updated_data["review"] is None:
                    del updated_data["review"]

            # Collect criteria from the GUI
            year_criteria = year_criteria_entry.get()
            rating_criteria = rating_criteria_entry.get()
            price_criteria = price_criteria_entry.get()
            criteria = {
                "review_date": int(year_criteria) if year_criteria else None,
                "rating": float(rating_criteria) if rating_criteria else None,
                "price": float(price_criteria) if price_criteria else None,
            }

            # Remove None values from criteria
            criteria = {k: v for k, v in criteria.items() if v is not None}

            # Call the update_key method
            self.selected_node.update_key(selected_country_key, updated_data, criteria)

            # Close the update window and refresh the GUI
            update_window.destroy()
            self.show_kd_tree_gui(selected_country, selected_country_key)
            self.update_info_panel(self.selected_node)

        # Create the update window
        update_window = tk.Toplevel(self.root)
        update_window.title("Update Coffee Shop Review")
        update_window.geometry("450x450")

        # Updated Data Section
        tk.Label(update_window, text="Updated Data", font=("Arial", 14)).grid(
            row=0, column=0, columnspan=2, pady=10
        )

        tk.Label(update_window, text="Year:", font=("Arial", 12)).grid(
            row=1, column=0, padx=10, pady=5, sticky="e"
        )
        year_entry = tk.Entry(update_window, font=("Arial", 12))
        year_entry.grid(row=1, column=1, padx=10, pady=5)

        tk.Label(update_window, text="Rating:", font=("Arial", 12)).grid(
            row=2, column=0, padx=10, pady=5, sticky="e"
        )
        rating_entry = tk.Entry(update_window, font=("Arial", 12))
        rating_entry.grid(row=2, column=1, padx=10, pady=5)

        tk.Label(update_window, text="Price (100g USD):", font=("Arial", 12)).grid(
            row=3, column=0, padx=10, pady=5, sticky="e"
        )
        price_entry = tk.Entry(update_window, font=("Arial", 12))
        price_entry.grid(row=3, column=1, padx=10, pady=5)

        tk.Label(update_window, text="Review:", font=("Arial", 12)).grid(
            row=4, column=0, padx=10, pady=5, sticky="ne"
        )
        review_text = tk.Text(update_window, font=("Arial", 12), width=30, height=4)
        review_text.grid(row=4, column=1, padx=10, pady=5)

        # Criteria Section
        tk.Label(update_window, text="Criteria", font=("Arial", 14)).grid(
            row=5, column=0, columnspan=2, pady=10
        )

        tk.Label(update_window, text="Year:", font=("Arial", 12)).grid(
            row=6, column=0, padx=10, pady=5, sticky="e"
        )
        year_criteria_entry = tk.Entry(update_window, font=("Arial", 12))
        year_criteria_entry.grid(row=6, column=1, padx=10, pady=5)

        tk.Label(update_window, text="Rating:", font=("Arial", 12)).grid(
            row=7, column=0, padx=10, pady=5, sticky="e"
        )
        rating_criteria_entry = tk.Entry(update_window, font=("Arial", 12))
        rating_criteria_entry.grid(row=7, column=1, padx=10, pady=5)

        tk.Label(update_window, text="Price (100g USD):", font=("Arial", 12)).grid(
            row=8, column=0, padx=10, pady=5, sticky="e"
        )
        price_criteria_entry = tk.Entry(update_window, font=("Arial", 12))
        price_criteria_entry.grid(row=8, column=1, padx=10, pady=5)

        # Submit Button
        submit_button = tk.Button(update_window, text="Submit", command=submit, font=("Arial", 12))
        submit_button.grid(row=9, column=0, columnspan=2, pady=10)

        # Bind the Return key to submit
        update_window.bind("<Return>", lambda event: submit())

    def delete_key_gui(self):
        """Delete a key from the network."""
        if self.selected_node is None:
            print("No node selected.")
            return

        # Check if the selected node has a KD-Tree
        if not hasattr(self.selected_node, "kd_tree") or not self.selected_node.kd_tree:
            print(f"Node {self.selected_node.node_id} does not have a KD Tree.")
            return

        # Prompt the user to select a country
        selected_country, selected_country_key = self.select_country_window()

        if not selected_country:
            print("Country selection canceled.")
            return

        # Confirm deletion with the user
        confirm_window = tk.Toplevel(self.root)
        confirm_window.title("Confirm Deletion")
        confirm_window.geometry("300x150")

        tk.Label(
            confirm_window,
            text=f"Delete all data for {selected_country}?",
            font=("Arial", 14),
        ).pack(pady=10)

        def confirm_delete():
            # Delete the key using the selected node
            self.selected_node.delete_key(selected_country_key)
            confirm_window.destroy()
            print(f"Deleted key for country: {selected_country}")
            # self.show_kd_tree_gui(selected_country, selected_country_key)
            self.show_dht_gui()

        def cancel_delete():
            confirm_window.destroy()
            print("Deletion canceled.")

        # Add confirmation and cancel buttons
        tk.Button(
            confirm_window,
            text="Confirm",
            command=confirm_delete,
            font=("Arial", 12),
        ).pack(side=tk.LEFT, padx=20, pady=10)

        tk.Button(
            confirm_window,
            text="Cancel",
            command=cancel_delete,
            font=("Arial", 12),
        ).pack(side=tk.RIGHT, padx=20, pady=10)

        # Bind Enter key to confirm deletion
        confirm_window.bind("<Return>", lambda event: confirm_delete())

    def lookup_key_gui(self):
        """Lookup similar reviews for a key in the network."""
        if self.selected_node is None:
            print("No node selected.")
            return

        # Check if the selected node has a KD-Tree
        if not hasattr(self.selected_node, "kd_tree") or not self.selected_node.kd_tree:
            print(f"Node {self.selected_node.node_id} does not have a KD Tree.")
            return

        # Prompt the user to select a country
        selected_country, selected_country_key = self.select_country_window()

        if not selected_country:
            print("Country selection canceled.")
            return

        # Create a window to input search ranges and N
        lookup_window = tk.Toplevel(self.root)
        lookup_window.title("Lookup Similar Reviews")
        lookup_window.geometry("450x400")

        # Year range
        tk.Label(lookup_window, text="Year Range:", font=("Arial", 12)).grid(
            row=0, column=0, padx=10, pady=5, sticky="e"
        )
        tk.Label(lookup_window, text="Lower:", font=("Arial", 12)).grid(
            row=1, column=0, padx=10, pady=5, sticky="e"
        )
        year_low_entry = tk.Entry(lookup_window, font=("Arial", 12))
        year_low_entry.grid(row=1, column=1, padx=10, pady=5)
        tk.Label(lookup_window, text="Upper:", font=("Arial", 12)).grid(
            row=2, column=0, padx=10, pady=5, sticky="e"
        )
        year_upper_entry = tk.Entry(lookup_window, font=("Arial", 12))
        year_upper_entry.grid(row=2, column=1, padx=10, pady=5)

        # Rating range
        tk.Label(lookup_window, text="Rating Range:", font=("Arial", 12)).grid(
            row=3, column=0, padx=10, pady=5, sticky="e"
        )
        tk.Label(lookup_window, text="Lower:", font=("Arial", 12)).grid(
            row=4, column=0, padx=10, pady=5, sticky="e"
        )
        rating_low_entry = tk.Entry(lookup_window, font=("Arial", 12))
        rating_low_entry.grid(row=4, column=1, padx=10, pady=5)
        tk.Label(lookup_window, text="Upper:", font=("Arial", 12)).grid(
            row=5, column=0, padx=10, pady=5, sticky="e"
        )
        rating_upper_entry = tk.Entry(lookup_window, font=("Arial", 12))
        rating_upper_entry.grid(row=5, column=1, padx=10, pady=5)

        # Price range
        tk.Label(lookup_window, text="Price Range (100g USD):", font=("Arial", 12)).grid(
            row=6, column=0, padx=10, pady=5, sticky="e"
        )
        tk.Label(lookup_window, text="Lower:", font=("Arial", 12)).grid(
            row=7, column=0, padx=10, pady=5, sticky="e"
        )
        price_low_entry = tk.Entry(lookup_window, font=("Arial", 12))
        price_low_entry.grid(row=7, column=1, padx=10, pady=5)
        tk.Label(lookup_window, text="Upper:", font=("Arial", 12)).grid(
            row=8, column=0, padx=10, pady=5, sticky="e"
        )
        price_upper_entry = tk.Entry(lookup_window, font=("Arial", 12))
        price_upper_entry.grid(row=8, column=1, padx=10, pady=5)

        # Number of similar reviews (N)
        tk.Label(lookup_window, text="Number of Similar Reviews (N):", font=("Arial", 12)).grid(
            row=9, column=0, padx=10, pady=5, sticky="e"
        )
        N_entry = tk.Entry(lookup_window, font=("Arial", 12))
        N_entry.grid(row=9, column=1, padx=10, pady=5)

        def submit():
            # Collect input values
            year_low = year_low_entry.get()
            year_upper = year_upper_entry.get()
            rating_low = rating_low_entry.get()
            rating_upper = rating_upper_entry.get()
            price_low = price_low_entry.get()
            price_upper = price_upper_entry.get()
            N = N_entry.get()

            # Validate inputs
            try:
                year_low = int(year_low) if year_low else None
                year_upper = int(year_upper) if year_upper else None
                rating_low = float(rating_low) if rating_low else None
                rating_upper = float(rating_upper) if rating_upper else None
                price_low = float(price_low) if price_low else None
                price_upper = float(price_upper) if price_upper else None
                N = int(N) if N else 5  # Default to 5 if N is not provided
            except ValueError:
                print("Invalid input. Please enter numeric values.")
                return

            # Ensure at least one range is provided
            if not (
                (year_low and year_upper)
                or (rating_low and rating_upper)
                or (price_low and price_upper)
            ):
                print("At least one range must be specified.")
                return

            # Execute the lookup operation
            response = self.selected_node.lookup(
                selected_country_key,
                lower_bounds=[year_low, rating_low, price_low],
                upper_bounds=[year_upper, rating_upper, price_upper],
                N=N,
            )
            if response["status"] == "failure":
                print("Lookup failed.")
                return
            points = response["points"]
            reviews = response["reviews"]
            similar_reviews = response["similar_reviews"]

            # Visualize the lookup results in the KD Tree 3d plot
            title = f"Lookup Results for {selected_country} in Range: {[year_low, rating_low, price_low]} - {[year_upper, rating_upper, price_upper]}"
            self.show_kd_tree_gui(
                selected_country,
                selected_country_key,
                points,
                reviews,
                title=title,
            )

            # Display results in the info panel
            self.node_info_label.config(text="LSH Similarity Search Results")
            self.info_text.config(state=tk.NORMAL)
            self.info_text.delete(1.0, tk.END)
            if response["status"] == "success" and len(similar_reviews) > 0:
                self.info_text.insert(tk.END, f"The {N} Most Similar Review:\n\n")
                for i, similar_review in enumerate(similar_reviews, 1):
                    self.info_text.insert(tk.END, f"{i}. {similar_review}\n\n")
            else:
                self.info_text.insert(tk.END, "No similar reviews found.\n")
            self.info_text.config(state=tk.DISABLED)

            # Close the lookup window
            lookup_window.destroy()

        # Submit button
        submit_button = tk.Button(lookup_window, text="Submit", command=submit, font=("Arial", 12))
        submit_button.grid(row=10, column=0, columnspan=2, pady=10)

        # Bind the Return key to submit
        lookup_window.bind("<Return>", lambda event: submit())
