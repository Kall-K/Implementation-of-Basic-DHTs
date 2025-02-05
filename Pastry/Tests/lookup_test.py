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

    # Define the lookup parameters
    lower_bounds = [2020, None, None]  # Example lower bounds
    upper_bounds = [2021, None, None]  # Example upper bounds
    N = 5  # Number of results to return

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
    total_lookups = 0

    # Perform lookups
    for key in unique_country_keys:
        # Select a random node for each lookup
        random_node = random.choice(list(network.nodes.values()))
        response = random_node.lookup(key, lower_bounds, upper_bounds, N=N)
        
        if response and "hops" in response:
            print(f"got a response for key {key}")
            total_hops += len(response["hops"])
            total_lookups += 1
            print(f"Lookup key: {key}, Hops: {response['hops']}, Node: {random_node.node_id}")
            print(f"Data: {response.get('data', 'No data found')}")

    # Calculate and print the average hops
    if total_lookups > 0:
        average_hops = total_hops / total_lookups
        print(f"Total Lookups Performed: {total_lookups}")
        print(f"Average hops per lookup: {average_hops}")
    else:
        print("No lookups were performed.")

    # Show the DHT GUI
    network.gui.show_dht_gui()
    network.gui.root.mainloop()


if __name__ == "__main__":
    main()