import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from Pastry.network import PastryNetwork
from constants import predefined_ids


def main():
    network = PastryNetwork()

    # network.build(node_num=20, dataset_path="../../../Coffee_Reviews_Dataset/simplified_coffee.csv")
    avg_join_hops, avg_insert_hops = network.build(
        predefined_ids=predefined_ids,
        dataset_path="../../../Coffee_Reviews_Dataset/simplified_coffee.csv",
    )

    print(f"\nAverage Hops during Node Arrivals: {avg_join_hops}")
    print(f"\nAverage Hops during key Insertion: {avg_insert_hops}\n")

    # Show the DHT GUI
    # network.gui.show_dht_gui()
    # network.gui.root.mainloop()


if __name__ == "__main__":
    main()
