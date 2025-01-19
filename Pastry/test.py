import time
import pandas as pd

import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from network import PastryNetwork
from node import PastryNode
from constants import *
from helper_functions import hash_key


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
    for node_id in predefined_ids:
        node = PastryNode(network, node_id=node_id)
        node.start_server()
        time.sleep(0.5)  # Allow the server to start
        network.node_join(node)
        print(f"Node Added: ID = {node.node_id}, Position = {node.position}")
    print("\nAll nodes have successfully joined the network.\n")

    # Stage 2: Key Insertions

    print("Stage 2: Key Insertions")
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

    # For similarity search testing later insert a custom entry in the USA
    country = "United States"
    name = "Sioutas' Coffee"
    key = hash_key(country)
    point = [2018, 94, 5.5]
    review = "Very delicate and sweet. Lemon verbena, dried persimmon, dogwood, baker's chocolate in aroma and cup. Balanced, sweet-savory structure; velvety-smooth mouthfeel. The sweetly herb-toned finish centers on notes of lemon verbena and dried persimmon wrapped in baker's chocolate."
    print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
    response = first_node.insert_key(key, point, review, country)
    print(response)

    # Insert all entries
    for key, point, review, country, name in zip(keys, points, reviews, countries, names):
        print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
        response = first_node.insert_key(key, point, review, country)
        print(response)

    # Inspect the state of each node
    print("\nInspecting the state of each node:")
    for node in network.nodes.values():
        node.print_state()

    # Stage 3: Key Lookup

    print("\nStage 3: Key Lookup")
    print("=======================")
    lookup_key = hash_key("United States")  # Hash the country name
    lower_bounds = [2017, 90, 4.0]
    upper_bounds = [2018, 95, 5.5]

    print(f"\nLooking up Key: {lookup_key}")
    response = first_node.lookup(lookup_key, lower_bounds, upper_bounds, N=5)
    print(response)

    # Stage 4: Key Deletion

    print("\nStage 4: Key Deletion")
    print("=======================")
    first_node.delete_key("372b")  # Delete the key for "United States"
    first_node.delete_key("6073")
    first_node.delete_key("4ca4")
    first_node.delete_key("aaaa")

    # Looup the United States key again to see if it was deleted
    # first_node.lookup(lookup_key, lower_bounds, upper_bounds, N=5)

    # Inspect the state of each node
    """print("\nInspecting the state of each node:")
    for node in network.nodes.values():
        node.print_state()"""


if __name__ == "__main__":
    main()
