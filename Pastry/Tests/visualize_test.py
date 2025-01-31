import time
import pandas as pd
import threading

import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from network import PastryNetwork
from node import PastryNode
from constants import *
from helper_functions import hash_key


# Builds a Pastry network and inserts all keys from the dataset
def main():
    print("=======================")
    print("Creating the Pastry network...")
    network = PastryNetwork()
    print("Node Joining")

    # 12 Predefined node IDs. As many as the countries in the dataset
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

    print(f"Adding {len(predefined_ids)} nodes to the network...")
    print("\n" + "-" * 100)
    for node_id in predefined_ids:
        node = PastryNode(network, node_id=node_id)
        print(f"Adding Node: ID = {node.node_id}")
        node.start_server()
        # time.sleep(0.1)  # Allow the server to start
        network.node_join(node)
        print(f"\nNode Added: ID = {node.node_id}, Position = {node.position}")
        print("\n" + "-" * 100)
    print("\nAll nodes have successfully joined the network.\n")

    # Insert keys
    # Load dataset
    dataset_path = "../../Coffee_Reviews_Dataset/simplified_coffee.csv"
    df = pd.read_csv(dataset_path)

    # Keep only the year from the review_date column
    df["review_date"] = pd.to_datetime(df["review_date"], format="%B %Y").dt.year

    # Extract loc_country as keys
    keys = df["loc_country"].apply(hash_key)

    # Extract data points (review_date, rating, 100g_USD)
    points = df[["review_date", "rating", "100g_USD"]].to_numpy()

    # Extract reviews and other details
    reviews = df["review"].to_numpy()
    countries = df["loc_country"].to_numpy()
    names = df["name"].to_numpy()

    print("Key Insertions")
    print("=======================")
    print("\nInserting data into the network...")
    first_node = list(network.nodes.values())[0]

    # Insert all entries
    for key, point, review, country, name in zip(keys, points, reviews, countries, names):
        print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
        response = first_node.insert_key(key, point, review, country)
        print(response)

    # Run the gui main loop
    network.gui.root.mainloop()


if __name__ == "__main__":
    main()
