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


def insert_keys(first_node, keys, points, reviews, countries, names):
    """Insert all keys sequentially."""
    for key, point, review, country, name in zip(keys, points, reviews, countries, names):
        print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
        response = first_node.insert_key(key, point, review, country)
        print(response)


def insert_key(node, key, point, review, country, name):
    """Insert a key."""
    print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
    response = node.insert_key(key, point, review, country)
    print(response)


def delete_key(node, key):
    """Delete a key."""
    print(f"\nDeleting Key: {key}\n")
    response = node.delete_key(key)
    print(response)


def lookup_key(node, lookup_key, lower_bounds, upper_bounds, N):
    """Lookup a key."""
    print(f"\nLooking up Key: {lookup_key}\n")
    response = node.lookup(lookup_key, lower_bounds, upper_bounds, N)
    print(response)


def main():
    # Load dataset
    dataset_path = "../../Coffee_Reviews_Dataset/simplified_coffee.csv"
    df = pd.read_csv(dataset_path)
    df["review_date"] = pd.to_datetime(df["review_date"], format="%B %Y").dt.year

    keys = df["loc_country"].apply(hash_key)
    points = df[["review_date", "rating", "100g_USD"]].to_numpy()
    reviews = df["review"].to_numpy()
    countries = df["loc_country"].to_numpy()
    names = df["name"].to_numpy()

    # Node Joining
    print("Node Joining")
    print("=======================")
    network = PastryNetwork()
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
    for node_id in predefined_ids:
        print("-" * 100)
        print(f"\nAdding Node: ID = {node_id}")
        node = PastryNode(network, node_id=node_id)
        node.start_server()
        time.sleep(0.1)
        network.node_join(node)
        print(f"Node Added: ID = {node.node_id}, Position = {node.position}")
    print("\nAll nodes have successfully joined the network.\n")

    # Key Insertions
    first_node = network.nodes[predefined_ids[0]]
    second_node = network.nodes[predefined_ids[1]]

    """print("Key Insertions")
    print("=======================")

    insert_keys(first_node, keys, points, reviews, countries, names)"""

    # Concurrent Operations
    print("\nConcurrent Operations")
    print("========================================")
    lookup_key_val = hash_key("United States")  # key = 372b, sto node 3722
    lower_bounds = [2017, 90, 4.0]
    upper_bounds = [2018, 95, 5.5]

    # delete_thread = threading.Thread(target=delete_key, args=(first_node, lookup_key_val))
    # lookup_thread = threading.Thread(target=lookup_key, args=(first_node, lookup_key_val, lower_bounds, upper_bounds, 5))

    # For similarity search testing later insert a custom entry in the USA
    country = "United States"
    name = "Greg's Coffee"
    key = hash_key(country)
    point = [2018, 94, 5.5]
    review = "Very delicate and sweet. Lemon verbena, dried persimmon, dogwood, baker's chocolate in aroma and cup. Balanced, sweet-savory structure; velvety-smooth mouthfeel. The sweetly herb-toned finish centers on notes of lemon verbena and dried persimmon wrapped in baker's chocolate."

    insert_key(first_node, key, point, review, country, name)
    insert_key(first_node, key, point, review, country, name)

    delete_thread = threading.Thread(target=delete_key, args=(first_node, key))
    delete_thread2 = threading.Thread(target=delete_key, args=(second_node, key))

    delete_thread.start()
    delete_thread2.start()

    delete_thread.join()
    delete_thread2.join()

    """lookup_thread.start()
    delete_thread.start()

    lookup_thread.join()
    delete_thread.join()"""

    """print("\nFinal State of Each Node:")
    for node in network.nodes.values():
        node.print_state()"""

    for node in network.nodes.values():
        node.thread_pool.shutdown(wait=True)


if __name__ == "__main__":
    main()
