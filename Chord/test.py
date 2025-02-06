import json
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import time

import threading

import pandas as pd

from Chord.network import ChordNetwork
from Chord.node import ChordNode
from constants import *
from helper_functions import *


def insert_keys(network, keys, points, reviews, countries, names):
    """Insert all keys sequentially."""
    responses = []
    for key, point, review, country, name in zip(keys, points, reviews, countries, names):
        # print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
        response = network.insert_key(key, point, review, country)
        responses.append(response["hops"])
    return responses


def delete_keys(network, keys):
    """Insert all keys sequentially."""
    responses = []
    for key in keys:
        response = network.delete_key(key)
        responses.append(response["hops"])
    return responses


def update_keys(network, keys):
    """Update all keys sequentially."""
    updated_data = {
        "point": [2018, 94, 5.5],
        "review": "New review text",
        "attributes": {"rating": 95},
    }
    hops = 0
    for key in keys:
        hops += network.update_key(key, updated_data)
    return hops / len(keys)


def lookups(network, keys):
    """Perform lookups for all keys."""
    N = 0
    lower_bounds = [2018, 0, 0]
    upper_bounds = [2018, 0, 0]
    hops = 0
    for key in keys:
        hops += network.lookup(key, lower_bounds, upper_bounds, N)
    return hops / len(keys)


def insert_key(network, key, point, review, country, name):
    """Insert a key."""
    # print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
    return network.insert_key(key, point, review, country)


def main():
    # Load dataset
    dataset_path = "../Coffee_Reviews_Dataset/simplified_coffee.csv"
    df = pd.read_csv(dataset_path)

    # Keep only the year from the review_date column
    df["review_date"] = pd.to_datetime(df["review_date"], format="%B %Y").dt.year

    # Extract loc_country as keys
    keys = df["loc_country"].apply(hash_key)
    unique_keys = list(set(keys))

    # Extract data points (review_date, rating, 100g_USD)
    points = df[["review_date", "rating", "100g_USD"]].to_numpy()

    # Extract reviews and other details
    reviews = df["review"].to_numpy()
    countries = df["loc_country"].to_numpy()
    names = df["name"].to_numpy()

    # Create the Chord network
    print("Creating the Chord network...")
    network = ChordNetwork()

    # List to store test results
    Results = {}

    print(
        """\n################################################################
#                         NODES JOIN                           #
################################################################\n"""
    )

    print(f"Adding {len(predefined_ids)} nodes to the network...")

    hops = []
    # Node Insertion
    for node_id in predefined_ids:
        node = ChordNode(network, node_id=node_id)
        node.start_server()
        hops.append(len(network.node_join(node)) - 1)

    print(f"Total num of hops to insert {len(hops)} nodes: {sum(hops)}")
    print(f"Avarage num of hops to insert a node: {sum(hops)/len(hops)}")
    Results[chord_operations[0]] = sum(hops) / len(hops)

    print(
        """\n################################################################
#                        KEYS INSERTION                        #
################################################################\n"""
    )
    print("\n" + "-" * 100 + "\n")
    print("-> Inserting Keys from Dataset...")
    result = insert_keys(network, keys, points, reviews, countries, names)
    print("-> End of Insertion")
    print(f"Average hops for Insertion: {sum(result)/len(result)}")
    print(f"Sum of hops: {sum(result)}")
    print(f"Total num of Inserts: {len(result)}")
    print("\n" + "-" * 100 + "\n")
    Results[chord_operations[1]] = sum(result) / len(result)

    print(
        """\n################################################################
#                        KEYS UPDATE                           #
################################################################\n"""
    )
    update_hops = update_keys(network, unique_keys)
    print("update hops: ", update_hops)
    Results[chord_operations[3]] = update_hops

    print(
        """\n################################################################
#                        KEYS LOOKUP                           #
################################################################\n"""
    )
    lookup_hops = lookups(network, unique_keys)
    print("lookup hops: ", lookup_hops)
    Results[chord_operations[4]] = lookup_hops

    print(
        """\n################################################################
#                        KEYS DELETION                         #
################################################################\n"""
    )
    print("\n" + "-" * 100 + "\n")
    print("-> Delete Keys from Dataset...")
    result = delete_keys(network, unique_keys)
    print("-> End of Deletion")
    print(f"Average hops for Deletion: {sum(result)/len(result)}")
    print(f"Sum of hops: {sum(result)}")
    print(f"Total num of Deletions: {len(result)}")
    print("\n" + "-" * 100 + "\n")
    Results[chord_operations[2]] = sum(result) / len(result)

    with open("ChordResults.json", "a") as outfile:
        json.dump(Results, outfile, indent=4)

    for node in predefined_ids:
        node = network.nodes[node]
        if node.running:
            node.leave()
            print("\n\n" + "-" * 100)
            time.sleep(5)
            # print(f"\n>>>> State after node {node.node_id} left")
            # for node in network.nodes.values():
            #     if node.running:
            #         node.print_state()

    running = True
    while running:
        time.sleep(2)
        running = False
        for node in network.nodes.values():
            if node.running:
                running = True


if __name__ == "__main__":
    main()
