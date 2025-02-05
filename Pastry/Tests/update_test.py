import sys
import os
import random
import numpy as np

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from Pastry.network import PastryNetwork
from Multidimensional_Data_Structures.kd_tree import KDTree
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

    # Build the network with predefined IDs
    network.build(
        predefined_ids=predefined_ids,
        dataset_path="../../Coffee_Reviews_Dataset/simplified_coffee.csv",
    )

    # Gather all unique country keys from each node's KD-Tree
    unique_country_keys = set()
    for node in network.nodes.values():
        if node.kd_tree is not None:
            keys, _ = node.kd_tree.get_unique_country_keys()
            unique_country_keys.update(keys)

    print(f"Unique country keys: {unique_country_keys}")

    unique_country_keys = list(unique_country_keys)
    random.shuffle(unique_country_keys)  # Shuffle to update keys randomly

    # Initialize variables to track hops
    total_hops = 0
    total_updates = 0

    # Update each key and count hops
    for key in unique_country_keys:
        # Define the update data
        update_to = {
            "review": f"An updated review for {key}'s coffee."
        }

        # Select a random node for each update
        random_node = random.choice(list(network.nodes.values()))
        update_response = random_node.update_key(key, updated_data=update_to)

        if update_response and "hops" in update_response:
            total_hops += len(update_response["hops"])
            total_updates += 1
            print(f"Updated key: {key}, Hops: {update_response['hops']}, Node: {random_node.node_id}")
        else:
            print(f"Failed to update key: {key}")

    # Calculate and print the average hops
    if total_updates > 0:
        average_hops = total_hops / total_updates
        print(f"Total Keys Updated: {total_updates}")
        print(f"Average hops per update: {average_hops}")
    else:
        print("No keys were updated.")

    # # Show the DHT GUI
    # network.gui.show_dht_gui()
    # network.gui.root.mainloop()


if __name__ == "__main__":
    main()