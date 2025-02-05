import threading

import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from Pastry.network import PastryNetwork
from Pastry.constants import *
from Pastry.helper_functions import hash_key


def insert_key(node, key, point, review, country, name):
    """Insert a key."""
    print(f"\nNode {node.node_id}: Inserting Key: {key}, Country: {country}, Name: {name}\n")
    response = node.insert_key(key, point, review, country)
    print(f"Node {node.node_id} Response: {response}")


def delete_key(node, key):
    """Delete a key."""
    print(f"\nNode {node.node_id}: Deleting Key: {key}\n")
    response = node.delete_key(key)
    print(f"Node {node.node_id} Response: {response}")


def lookup_key(node, lookup_key, lower_bounds, upper_bounds, N):
    """Lookup a key."""
    print(f"\nNode {node.node_id}: Looking up Key: {lookup_key}\n")
    response = node.lookup(lookup_key, lower_bounds, upper_bounds, N)
    print(f"Node {node.node_id} Response: {response}")


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

    # Build the network with random node_num nodes
    # network.build(node_num=20, dataset_path="../../Coffee_Reviews_Dataset/simplified_coffee.csv")

    # Build the network with predefined IDs
    network.build(
        predefined_ids=predefined_ids,
        dataset_path="../../Coffee_Reviews_Dataset/simplified_coffee.csv",
    )

    # Concurrent Operations
    print("\nConcurrent Operations")
    print("========================================")

    """
    One thread must be created for each operation to be performed concurrently.
    For example, for two concurrent insert operations:
    insert_thread1 = threading.Thread(target=insert_key, args=(some_node, key1, point1, review1, country1, name1))
    insert_thread2 = threading.Thread(target=insert_key, args=(some_node, key2, point2, review2, country2, name2))

    insert_thread1.start()
    insert_thread2.start()

    insert_thread1.join()
    insert_thread2.join()
    
    Where some_node is a node object from network.nodes, keyx = hash_key(countryx),
    pointx = [year, rating, price], reviewx = "Review", countryx = "Country", namex = "Name"
    """

    """--- Concurrent Insert Operations ---"""

    country = "United States"
    name = "Greg's Coffee"
    key = hash_key(country)
    point = [2018, 94, 5.5]
    review = "Very delicate and sweet. Lemon verbena, dried persimmon, dogwood, baker's chocolate in aroma and cup."

    node1 = list(network.nodes.values())[0]
    node2 = list(network.nodes.values())[1]

    insert_thread1 = threading.Thread(
        target=insert_key, args=(node1, key, point, review, country, name)
    )
    insert_thread2 = threading.Thread(
        target=insert_key, args=(node2, key, point, review, country, name)
    )

    insert_thread1.start()
    insert_thread2.start()

    insert_thread1.join()
    insert_thread2.join()

    """lookup_key_val = hash_key("United States")  # key = 372b, sto node 3722
    lower_bounds = [2017, 90, 4.0]
    upper_bounds = [2018, 95, 5.5]

    # delete_thread = threading.Thread(target=delete_key, args=(first_node, lookup_key_val))
    # lookup_thread = threading.Thread(target=lookup_key, args=(first_node, lookup_key_val, lower_bounds, upper_bounds, 5))

    # For similarity search testing later insert a custom entry in the USA

    insert_key(first_node, key, point, review, country, name)
    insert_key(first_node, key, point, review, country, name)

    delete_thread = threading.Thread(target=delete_key, args=(first_node, key))
    delete_thread2 = threading.Thread(target=delete_key, args=(second_node, key))

    delete_thread.start()
    delete_thread2.start()

    delete_thread.join()
    delete_thread2.join()

    lookup_thread.start()
    delete_thread.start()

    lookup_thread.join()
    delete_thread.join()

    print("\nFinal State of Each Node:")
    for node in network.nodes.values():
        node.print_state()"""

    # Show the DHT GUI
    network.gui.show_dht_gui()
    network.gui.root.mainloop()


if __name__ == "__main__":
    main()
