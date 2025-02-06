import tkinter as tk
from tkinter import simpledialog

from Pastry.network import PastryNetwork
from Chord.network import ChordNetwork
from constants import predefined_ids

WIDTH = 640
HEIGHT = 360


class MainLauncher(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("DHT Centralized Dashboard")
        self.geometry(f"{WIDTH}x{HEIGHT}")

        self.protocol("WM_DELETE_WINDOW", self.on_close)

        label = tk.Label(self, text="Select which DHT to build:", font=("Arial", 30))
        label.pack(pady=30)

        # Create two buttons for each DHT
        btn_pastry = tk.Button(
            self,
            text="Build Pastry",
            command=self.launch_pastry,
            font=("Arial", 20),
        )
        btn_pastry.pack(pady=20)

        btn_chord = tk.Button(
            self,
            text="Build Chord",
            command=self.launch_chord,
            font=("Arial", 20),
        )
        btn_chord.pack(pady=10)

    def on_close(self):
        """Stop the main loop and destroy the main window when closed."""
        print("\nClosing the application...")
        self.quit()
        self.destroy()

    def prompt_num_nodes(self):
        """Prompt the user to enter the number of nodes for random creation."""
        return simpledialog.askinteger(
            "Number of Nodes",
            "Enter the number of nodes to create:",
            parent=self,
            minvalue=1,
            maxvalue=100,
        )

    def launch_pastry(self):
        """Launch the dialog to select node creation method for Pastry."""
        dialog = tk.Toplevel(self)
        dialog.title("Pastry Node Creation")
        dialog.geometry("300x150")

        label = tk.Label(dialog, text="Choose node creation method:", font=("Arial", 15))
        label.pack(pady=10)

        btn_predefined = tk.Button(
            dialog,
            text="Predefined Nodes",
            command=lambda: self.handle_pastry_choice(dialog, predefined=True),
            font=("Arial", 12),
        )
        btn_predefined.pack(pady=5)

        btn_random = tk.Button(
            dialog,
            text="Random Nodes",
            command=lambda: self.handle_pastry_choice(dialog, predefined=False),
            font=("Arial", 12),
        )
        btn_random.pack(pady=5)

        self.wait_window(dialog)

    def handle_pastry_choice(self, dialog, predefined):
        """Handle the user's choice for Pastry node creation."""
        dialog.destroy()

        if predefined:
            # Build with predefined nodes
            self.withdraw()
            pastry_network = PastryNetwork(self)
            pastry_network.build(predefined_ids)
            pastry_network.gui.show_dht_gui()
            pastry_network.gui.root.mainloop()
        else:
            # Prompt for number of nodes
            num_nodes = self.prompt_num_nodes()
            if num_nodes is not None:
                self.withdraw()
                pastry_network = PastryNetwork(self)
                pastry_network.build(node_num=num_nodes)
                pastry_network.gui.show_dht_gui()
                pastry_network.gui.root.mainloop()

    def launch_chord(self):
        """Launch the dialog to select node creation method for Chord."""
        dialog = tk.Toplevel(self)
        dialog.title("Chord Node Creation")
        dialog.geometry("300x150")

        label = tk.Label(dialog, text="Choose node creation method:", font=("Arial", 15))
        label.pack(pady=10)

        btn_predefined = tk.Button(
            dialog,
            text="Predefined Nodes",
            command=lambda: self.handle_chord_choice(dialog, predefined=True),
            font=("Arial", 12),
        )
        btn_predefined.pack(pady=5)

        btn_random = tk.Button(
            dialog,
            text="Random Nodes",
            command=lambda: self.handle_chord_choice(dialog, predefined=False),
            font=("Arial", 12),
        )
        btn_random.pack(pady=5)

        self.wait_window(dialog)

    def handle_chord_choice(self, dialog, predefined):
        """Handle the user's choice for Chord node creation."""
        dialog.destroy()

        if predefined:
            # Build with predefined nodes
            self.withdraw()
            chord_network = ChordNetwork(self)
            chord_network.build(predefined_ids)
            chord_network.gui.show_dht_gui()
            chord_network.gui.root.mainloop()
        else:
            # Prompt for number of nodes
            num_nodes = self.prompt_num_nodes()
            if num_nodes is not None:
                self.withdraw()
                chord_network = ChordNetwork(self)
                chord_network.build(node_num=num_nodes)
                chord_network.gui.show_dht_gui()
                chord_network.gui.root.mainloop()


if __name__ == "__main__":
    app = MainLauncher()
    app.mainloop()
