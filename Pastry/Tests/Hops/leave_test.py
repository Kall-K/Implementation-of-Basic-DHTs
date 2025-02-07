import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from Pastry.network import PastryNetwork
from Multidimensional_Data_Structures.kd_tree import KDTree
from constants import predefined_ids


def leave_test(network=None):
    if network is None:
        network = PastryNetwork()

        # Build the network with predefined IDs
        network.build(
            predefined_ids=predefined_ids,
            dataset_path="../../../Coffee_Reviews_Dataset/simplified_coffee.csv",
        )

    node_ids = list(network.nodes.keys())  # Create a static copy list of node IDs
    num_nodes = len(node_ids)  # Original node count
    num_leave_hops = 0
    for node_id in node_ids:
        response = network.leave(node_id)
        if response and "hops" in response:
            num_leave_hops += len(response["hops"])
    avg_leave_hops = num_leave_hops / num_nodes

    print(f"\nAverage hops for Graceful Node Departures: {avg_leave_hops}")

    return avg_leave_hops

    # Show the DHT GUI
    # network.gui.show_dht_gui()
    # network.gui.root.mainloop()


if __name__ == "__main__":
    leave_test()
