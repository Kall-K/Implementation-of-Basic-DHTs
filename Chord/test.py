import time

import threading

import pandas as pd

from network import ChordNetwork
from node import ChordNode
from constants import *
from helper_functions import *


def insert_keys(network, keys, points, reviews, countries, names):
    """Insert all keys sequentially."""
    for key, point, review, country, name in zip(keys, points, reviews, countries, names):
        # print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
        network.insert_key(key, point, review, country)


def insert_key(network, key, point, review, country, name):
    """Insert a key."""
    # print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")
    return network.insert_key(key, point, review, country)


def main():
    # Load dataset
    dataset_path = "../Coffee_Reviews_Dataset/simplified_coffee.csv"
    df = pd.read_csv(dataset_path)
    # df = df[:75]

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

    ################################################################
    #                         NODES JOIN                           #
    ################################################################

    print(
        """\n################################################################
#                         NODES JOIN                           #
################################################################\n"""
    )

    # Predefined node IDs to test
    predefined_ids = ["4b12", "fa35", "19bd", "4bde", "4c12", "cafe"]
    # predefined_ids = [
    #         "4b12",
    #         "fa35",
    #         "19bd",
    #         "37de",
    #         "3722",
    #         "ca12",
    #         "cafe",
    #         "fb32",
    #         "20bc",
    #         "20bd",
    #         "3745",
    #         "d3ad",
    #     ]
    print(f"Adding {len(predefined_ids)} nodes to the network...")

    # Node Insertion
    for node_id in predefined_ids:
        node = ChordNode(network, node_id=node_id)
        node.start_server()
        network.node_join(node)
        # node.print_state()

    # network.visualize_network()
    # time.sleep(5)
    # network.nodes["4b12"].leave()
    # time.sleep(5)
    # for node_id in network.nodes.keys():
    #     if network.nodes[node_id].running:
    #         network.nodes[node_id].print_state()

    ################################################################
    #                        KEYS INSERTION                        #
    ################################################################
    print(
        """\n################################################################
#                        KEYS INSERTION                        #
################################################################\n"""
    )
    print("\n" + "-" * 100 + "\n")
    print("-> Inserting Keys from Dataset...")
    insert_keys(network, keys, points, reviews, countries, names)
    print("-> End of Insertion")
    print("\n" + "-" * 100 + "\n")

#     ################################################################
#     #                        DEMOSTRATION                          #
#     ################################################################
#     print(
#         """\n################################################################
# #                        INSERT KEY                            #
# ################################################################\n"""
#     )
#     country = "Romania "
#     name = "Carpathian Coffee"
#     key = hash_key(country)
#     point = [2023, 97, 5.7]
#     review = "Dried plums, acacia honey, roasted walnuts, and dark chocolate in aroma and cup. Fine, well-balanced acidity; creamy and rich body. Long finish with notes of caramelized almonds and subtle essences of vanilla and smoked oak."
#     response = insert_key(network, key, point, review, country, name)
#     print(f">> Inserting Key: {key}, Country: {country}, Name: {name}")
#     print(f">> Insertion status: {response['status']}.")
#     print(f">> {response['message']}.")
#     print(f">> Key Inserted with {response['hops']} hops.")
#     ################################################################
#     #                        LOOKUP KEY                            #
#     ################################################################
#     lower_bounds = [2023, 97, 5.7]
#     upper_bounds = [2023, 97, 5.7]
#     print("\nVerifying insertion through lookup: ")
#     response = network.lookup(key, lower_bounds, upper_bounds, N=5)
#     print(f">> Lookup status: {response['status']}.")
#     print(f">> {response['message']}")
#     print(f">> Key Found with {response['hops']} hops.")
#     # ################################################################
#     # #                        UPDATE KEY                            #
#     # ################################################################
#     print(
#         """\n################################################################
# #                        UPDATE KEY                            #
# ################################################################\n""")
#     # Update all points
#     print("\nUpdating all points for Romania:\n")
#     update_fields = {"attributes": {"price": 35.0}}
#     network.update_key(key, updated_data=update_fields)
    
#     # Update only the review
#     print("\nUpdate In Parallel")
#     print("Updating only the review for Taiwan:\n")
#     update_fields1 = {
#         "review": "An updated review for Romania's coffee: Dried plums, acacia honey, roasted walnuts, and dark chocolate in aroma and cup."
#     } 
#     update_fields2 = {
#         "review": "An 2nd updated review for Romania's coffee: crisp and fruity with a lingering sweetness."
#     }
 
#     update_thread1 = threading.Thread(target=network.update_key, args=(key, update_fields1,))
#     update_thread2 = threading.Thread(target=network.update_key, args=(key, update_fields2,))

#     update_thread2.start()
#     update_thread1.start()

#     update_thread1.join()
#     update_thread2.join()
#     # ################################################################
#     # #                        LOOKUP KEY                            #
#     # ################################################################
#     lower_bounds = [2000, 10, 0]
#     upper_bounds = [2023, 100, 50]
#     print("\nVerifying updates through lookup:")
#     response = network.lookup(key, lower_bounds, upper_bounds, N=5)
#     print(f">> Lookup status: {response["status"]}.")
#     print(f">> {response["message"]}")
#     print(f">> Key Found with {response["hops"]} hops.")
#     # ################################################################
#     # #                        DELETE KEY                            #
#     # ################################################################
#     print("""\n################################################################
# #                        DELETE KEY                            #
# ################################################################\n""")
#     taiwan_country_key = hash_key("Taiwan")
#     print(f"\nDelete key with value {taiwan_country_key}.")
#     response = network.delete_key(taiwan_country_key)
#     print(f">> Delete status: {response["status"]}.")
#     print(f">> {response["message"]}")
#     print(f">> Key Deleted with {response["hops"]} hops.")
#     # ################################################################
#     # #                        LOOKUP KEY                            #
#     # ################################################################
#     lower_bounds = [2018, 90, 30.0]
#     upper_bounds = [2019, 95, 40.0]
#     print("\nVerifying deletion through lookup:\n")
#     network.lookup(taiwan_country_key, lower_bounds, upper_bounds, N=5)

    for node_id in network.nodes.keys():
        if network.nodes[node_id].running:
            network.nodes[node_id].print_state()

    node = ChordNode(network, node_id="2fec")
    node.start_server()
    network.node_join(node)
    # network.nodes["4b12"].leave()
    time.sleep(5)
    for node_id in network.nodes.keys():
        if network.nodes[node_id].running:
            network.nodes[node_id].print_state()

    # ################################################################
    # #                        NODES LEAVE                           #
    # ################################################################    
    print("""\n################################################################
#                        NODES LEAVE                           #
################################################################\n"""
    )
    time.sleep(2)
    nodes_to_leave = ["19bd", "fa35"]
    for node in nodes_to_leave:
        node = network.nodes[node]
        if node.running:
            node.leave()
            print("\n\n" + "-" * 100)
            time.sleep(5)
            print(f"\n>>>> State after node ${node.node_id} left")
            for node in network.nodes.values():
                if node.running:
                    node.print_state()

    # network.nodes["4bde"].leave()
    # network.nodes["4c12"].leave()
    # network.nodes["cafe"].leave()
    # network.nodes["2fec"].leave()
    

    running = True
    while running:
        time.sleep(2)
        running = False
        for node in network.nodes.values():
            if node.running:
                running = True


if __name__ == "__main__":
    main()
