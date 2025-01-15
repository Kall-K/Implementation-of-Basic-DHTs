import time
import pandas as pd
import hashlib

import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from network import PastryNetwork
from node import PastryNode
from constants import *


def hash_key(value):
    """
    Hash the input value and return the least 4 hex digits.
    """
    sha1_hash = hashlib.sha1(value.encode()).hexdigest()
    return sha1_hash[-4:]


def main():
    # Load dataset
    dataset_path = "../Coffee_Reviews_Dataset/simplified_coffee.csv"
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

    # Stage 1: Node Joining
    print("Stage 1: Node Joining")
    print("=======================")
    print("Creating the Pastry network...")
    network = PastryNetwork()

    # Predefined node IDs
    predefined_ids = ["4b12", "fa35", "19bd", "37de", "3722", "cafe"]

    print(f"Adding {len(predefined_ids)} nodes to the network...")
    for node_id in predefined_ids:
        node = PastryNode(network, node_id=node_id)
        node.start_server()
        time.sleep(1)  # Allow the server to start
        network.node_join(node)
        print(f"Node Added: ID = {node.node_id}, Address = {node.address}")
    print("\nAll nodes have successfully joined the network.\n")

    # Stage 2: Key Insertion
    print("Stage 2: Key Insertion")
    print("=======================")
    print("\nInserting data into the network...")
    first_node = list(network.nodes.values())[0]

    # # Insert only the first 10 entries for step-by-step hops
    # print("\nInserting the first 10 entries into the network...\n")
    # for i in range(10):
    #     key, point, review, country, name = keys[i], points[i], reviews[i], countries[i], names[i]
    #     print(f"Inserting Key: {key}, Country: {country}, Name: {name}")
    #     print("")
    #     first_node.insert_key(key, point, review)

    # Insert all entries
    for key, point, review, country, name in zip(
        keys, points, reviews, countries, names
    ):
        print(f"\nInserting Key: {key}, Country: {country}, Name: {name}")
        first_node.insert_key(key, point, review)

    # Inspect the state of each node
    print("\nInspecting the state of each node:")
    for node in network.nodes.values():
        node.print_state()
        # if node.kd_tree and node.kd_tree.points.size > 0:
        #     print(f"KDTree for Node {node.node_id}:")
        #     node.kd_tree.print_search_results(node.kd_tree.points, node.kd_tree.reviews)

    # Print the KDTree of the first node
    """if first_node.kd_tree and first_node.kd_tree.points.size > 0:
        print("\nKDTree for the first node (ID: {first_node.node_id}):")
        first_node.kd_tree.print_search_results(
            first_node.kd_tree.points, first_node.kd_tree.reviews
        )"""


if __name__ == "__main__":
    main()
