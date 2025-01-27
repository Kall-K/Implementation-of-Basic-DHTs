import time
import pandas as pd
from network import ChordNetwork
from node import ChordNode
from constants import *
from helper_functions import *


def insert_keys(network, keys, points, reviews, countries, names):
    """Insert all keys sequentially."""
    for key, point, review, country, name in zip(keys, points, reviews, countries, names):
        print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
        network.insert_key(key, point, review, country)
        
# def insert_key(network, key, point, review, country, name):
#     """Insert a key."""
#     print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
#     response = network.insert_key(key, point, review, country)
#     print(response)



def main():
    # Load dataset
    dataset_path = "../Coffee_Reviews_Dataset/simplified_coffee.csv"
    df = pd.read_csv(dataset_path)
    df = df[10:15]
    print(df)
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

    # Create the Chord network
    print("Creating the Chord network...")
    network = ChordNetwork()

    # Predefined node IDs to test
    predefined_ids = ["4b12", "fa35", "19bd", "4bde", "4c12", "cafe"]

    print(f"Adding {len(predefined_ids)} nodes to the network...")

    # Node Insertion
    for node_id in predefined_ids:
        # Create a ChordNode with a specific ID
        node = ChordNode(network, node_id=node_id)
        node.start_server()
        time.sleep(1)  # Allow the server to start
        network.node_join(node)
        print(f"Node Added: ID = {node.node_id}, Address = {node.address}")
        #node.print_state()
    
    # Key Insertion
    print("\nKey Insertions")
    print("=======================")
    insert_keys(network, keys, points, reviews, countries, names)
    # country = "United States"
    # name = "Greg's Coffee"
    # key = hash_key(country)
    # point = [2018, 94, 5.5]
    # review = "Very delicate and sweet. Lemon verbena, dried persimmon, dogwood, baker's chocolate in aroma and cup. Balanced, sweet-savory structure; velvety-smooth mouthfeel. The sweetly herb-toned finish centers on notes of lemon verbena and dried persimmon wrapped in baker's chocolate."

    # insert_key(first_node, key, point, review, country, name)
    # Key Deletion
    print("\nKey Deletion")
    print("=======================")
    network.delete_key('372a')
    network.delete_key('372b')

    # Key Update
    print("\nKey Update")
    print("=======================")

    taiwan_country_key = hash_key("Taiwan")

    # Update all points for Taiwan
    print("\nUpdating all points for Taiwan:\n")
    update_fields = {"attributes": {"price": 35.0}}
    network.update_key(key=taiwan_country_key, updated_data=update_fields)

    # Update a specific point for Taiwan
    print("\nUpdating a specific point for Taiwan:\n")
    criteria = {"review_date": 2019, "rating": 94, "price": 35.0}
    update_fields = {"attributes": {"price": 36.0}}
    network.update_key(key=taiwan_country_key, updated_data=update_fields, criteria=criteria)

    # Update only the review for Taiwan
    print("\nUpdating only the review for Taiwan:\n")
    update_fields = {
        "review": "An updated review for Taiwan's coffee: crisp and fruity with a lingering sweetness."
    }
    network.update_key(key=taiwan_country_key, updated_data=update_fields)

    # Update based on specific attributes and modify multiple fields
    print("\nUpdating specific attributes for Taiwan:\n")
    criteria = {"review_date": 2019, "rating": 94}
    update_fields = {"attributes": {"price": 37.0, "rating": 95}}
    network.update_key(key=taiwan_country_key, updated_data=update_fields, criteria=criteria)

    # # Verify all updates
    # lower_bounds = [2018, 90, 30.0]
    # upper_bounds = [2019, 95, 40.0]
    # print("\nVerifying updates through lookup:\n")
    # response = network.lookup(taiwan_country_key, lower_bounds, upper_bounds, N=5)
    # print(response)


if __name__ == "__main__":
    main()