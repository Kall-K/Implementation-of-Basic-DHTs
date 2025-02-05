import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from Pastry.network import PastryNetwork

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


def main():
    network = PastryNetwork()

    # network.build(node_num=20, dataset_path="../../Coffee_Reviews_Dataset/simplified_coffee.csv")
    network.build(
        predefined_ids=predefined_ids,
        dataset_path="../../Coffee_Reviews_Dataset/simplified_coffee.csv",
    )

    # Delete a key
    node = list(network.nodes.values())[0]  # First Node
    node.delete_key("dc5e")

    network.gui.show_dht_gui()
    network.gui.root.mainloop()


if __name__ == "__main__":
    main()
