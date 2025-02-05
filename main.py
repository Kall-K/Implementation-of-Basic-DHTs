import tkinter as tk

from Pastry.network import PastryNetwork
from Chord.network import ChordNetwork

WIDTH = 640
HEIGHT = 360


class MainLauncher(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("DHT Centralized Dashboard")
        self.geometry(f"{WIDTH}x{HEIGHT}")

        self.protocol("WM_DELETE_WINDOW", self.on_close)

        # Create a label or any instructions you like
        label = tk.Label(self, text="Select which DHT to build:", font=("Arial", 30))
        label.pack(pady=30)

        # 12 Predefined node IDs.
        predefined_ids = [
            "4b12",
            "fa35",
            "19bd",
            "37de",
            "3722",
            "ca12",
            "cafe",
            "fb32",
            "20bc",
            "20bd",
            "3745",
            "d3ad",
        ]

        # Create two buttons for each DHT
        btn_pastry = tk.Button(
            self,
            text="Build Pastry",
            command=lambda: self.launch_pastry(predefined_ids),
            font=("Arial", 20),
        )
        btn_pastry.pack(pady=20)

        btn_chord = tk.Button(
            self,
            text="Build Chord",
            command=lambda: self.launch_chord(predefined_ids),
            font=("Arial", 20),
        )
        btn_chord.pack(pady=10)

    def on_close(self):
        """Stop the main loop and destroy the main window when closed."""
        print("\nClosing the application...")
        self.quit()  # Stop the mainloop
        self.destroy()  # Destroy the window

    def launch_pastry(self, predefined_ids):
        # Create the pastry network
        pastry_network = PastryNetwork(self)

        # Hide the main window
        self.withdraw()

        # Build the network
        pastry_network.build(predefined_ids)
        # Show the Pastry GUI
        pastry_network.gui.show_dht_gui()
        # Run the gui main loop
        pastry_network.gui.root.mainloop()

    def launch_chord(self, predefined_ids):
        # Create the chord network
        chord_network = ChordNetwork(self)

        # Hide the main window
        self.withdraw()

        # Build the network
        chord_network.build(predefined_ids)
        # Show the Chord GUI
        chord_network.gui.show_dht_gui()
        # Run the gui main loop
        chord_network.gui.root.mainloop()


if __name__ == "__main__":
    app = MainLauncher()
    app.mainloop()
