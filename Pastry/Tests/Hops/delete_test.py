import sys
import os
import random

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from Pastry.network import PastryNetwork
from Multidimensional_Data_Structures.kd_tree import KDTree
from constants import predefined_ids


def delete_test(network=None):
    if network is None:
        network = PastryNetwork()

        # Build the network with predefined IDs
        network.build(
            predefined_ids=predefined_ids,
            dataset_path="../../../Coffee_Reviews_Dataset/simplified_coffee.csv",
        )

    # Gather all unique country keys from each node's KD-Tree
    unique_country_keys = set()
    for node in network.nodes.values():
        if node.kd_tree is not None:
            keys, _ = node.kd_tree.get_unique_country_keys()
            unique_country_keys.update(keys)

    print(f"Unique country keys: {unique_country_keys}")

    unique_country_keys = list(unique_country_keys)
    random.shuffle(unique_country_keys)  # Shuffle to delete keys randomly

    # Initialize variables to track hops
    total_hops = 0
    total_deletions = 0

    # Delete each key and count hops
    for key in unique_country_keys:
        # Select a random node for each deletion
        random_node = random.choice(list(network.nodes.values()))
        response = random_node.delete_key(key)

        if response and "hops" in response:
            total_hops += len(response["hops"])
            total_deletions += 1
            print(f"Deleted key: {key}, Hops: {response['hops']}, Node: {random_node.node_id}")

    # Calculate and print the average hops
    if total_deletions > 0:
        average_hops = total_hops / total_deletions
        print(f"\nTotal Keys Deleted: {total_deletions}")
        print(f"Average hops per deletion: {average_hops}")
    else:
        print("No keys were deleted.")

    return average_hops

    # # Show the DHT GUI
    # network.gui.show_dht_gui()
    # network.gui.root.mainloop()


if __name__ == "__main__":
    delete_test()
