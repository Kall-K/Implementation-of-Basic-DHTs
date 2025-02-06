import threading

import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from Pastry.network import PastryNetwork
from Pastry.node import PastryNode
from Pastry.constants import *
from Pastry.helper_functions import hash_key
from constants import predefined_ids


def insert_key(node, key, point, review, country, name):
    """Insert a key."""
    thread_id = threading.get_ident()
    print(
        f"\n[Thread {thread_id}] Node {node.node_id}: Inserting Key: {key}, Country: {country}, Name: {name}\n"
    )
    response = node.insert_key(key, point, review, country)
    print(f"[Thread {thread_id}] Node {node.node_id} Response: {response}")


def update_key(node, key, updated_data, criteria):
    """Update a key"""
    thread_id = threading.get_ident()
    print(f"\n[Thread {thread_id}] Node {node.node_id}: Updating Key: {key}\n")
    response = node.update_key(key, updated_data, criteria)
    print(f"[Thread {thread_id}] Node {node.node_id} Response: {response}")


def delete_key(node, key):
    """Delete a key."""
    thread_id = threading.get_ident()
    print(f"\n[Thread {thread_id}] Node {node.node_id}: Deleting Key: {key}\n")
    response = node.delete_key(key)
    print(f"[Thread {thread_id}] Node {node.node_id} Response: {response}")


def lookup_key(node, lookup_key, lower_bounds, upper_bounds, N):
    """Lookup a key."""
    thread_id = threading.get_ident()
    print(f"\n[Thread {thread_id}] Node {node.node_id}: Looking up Key: {lookup_key}\n")
    response = node.lookup(lookup_key, lower_bounds, upper_bounds, N)
    # print(f"[Thread {thread_id}] Node {node.node_id} Response: {response}")


def join(network, node_id):
    """Join a node to the network."""
    thread_id = threading.get_ident()
    print(f"\n[Thread {thread_id}] Node {node_id}: Joining the network.\n")
    node = PastryNode(network, node_id)
    node.start_server()
    response = network.node_join(node)
    print(f"[Thread {thread_id}] Network Response: {response}")


def leave(network, node_id):
    """Remove a node from the network"""
    thread_id = threading.get_ident()
    print(f"\n[Thread {thread_id}] Node {node_id}: Leaving the network gracefully.\n")
    response = network.leave(node_id)
    print(f"[Thread {thread_id}] Network Response: {response}")


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
    Explanation:
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

    """country1 = "United States"
    name1 = "Greg's Coffee"
    key1 = hash_key(country1)
    point1 = [2018, 94, 5.4]
    review1 = "Very delicate and sweet. Lemon verbena, dried persimmon, dogwood, baker's chocolate in aroma and cup."

    country2 = "United States"
    name2 = "Dam's Coffee"
    key2 = hash_key(country2)
    point2 = [2018, 94, 5.5]
    review2 = "Very delicate and sweet. Lemon verbena, dried persimmon, dogwood, baker's chocolate in aroma and cup."

    node1 = list(network.nodes.values())[0]
    node2 = list(network.nodes.values())[1]

    insert_thread1 = threading.Thread(
        target=insert_key, args=(node1, key1, point1, review1, country1, name1)
    )
    insert_thread2 = threading.Thread(
        target=insert_key, args=(node2, key2, point2, review2, country2, name2)
    )

    insert_thread1.start()
    insert_thread2.start()

    insert_thread1.join()
    insert_thread2.join()"""

    """--- Concurrent Update Operations ---"""
    """key1 = hash_key("United States")
    updated_data1 = {"review": f"Updated Review 1."}

    key2 = hash_key("United States")
    updated_data2 = {"review": f"Updated Review 2"}

    node1 = list(network.nodes.values())[0]
    node2 = list(network.nodes.values())[1]

    update_thread1 = threading.Thread(target=update_key, args=(node1, key1, updated_data1, None))
    update_thread2 = threading.Thread(target=update_key, args=(node2, key2, updated_data2, None))

    update_thread1.start()
    update_thread2.start()

    update_thread1.join()
    update_thread2.join()"""

    """--- Concurrent Delete Operations ---"""
    """key1 = hash_key("United States")
    key2 = hash_key("United States")

    node1 = list(network.nodes.values())[0]
    node2 = list(network.nodes.values())[1]

    delete_thread1 = threading.Thread(target=delete_key, args=(node1, key1))
    delete_thread2 = threading.Thread(target=delete_key, args=(node2, key2))

    delete_thread1.start()
    delete_thread2.start()

    delete_thread1.join()
    delete_thread2.join()"""

    """--- Concurrent Lookup Operations ---"""
    """key1 = hash_key("United States")
    lower_bounds1 = [2020, None, None]
    upper_bounds1 = [2021, None, None]

    key2 = hash_key("United States")
    lower_bounds2 = [2020, None, None]
    upper_bounds2 = [2021, None, None]

    node1 = list(network.nodes.values())[0]
    node2 = list(network.nodes.values())[1]

    lookup_thread1 = threading.Thread(
        target=lookup_key, args=(node1, key1, lower_bounds1, upper_bounds1, None)
    )
    lookup_thread2 = threading.Thread(
        target=lookup_key, args=(node2, key2, lower_bounds2, upper_bounds2, None)
    )

    lookup_thread1.start()
    lookup_thread2.start()

    lookup_thread1.join()
    lookup_thread2.join()"""

    """--- Concurrent Node Arrivals ---"""
    """node_id1 = "1765"
    node_id2 = "5678"

    join_thread1 = threading.Thread(target=join, args=(network, node_id1))
    join_thread2 = threading.Thread(target=join, args=(network, node_id2))

    join_thread1.start()
    join_thread2.start()

    join_thread1.join()
    join_thread2.join()"""

    """--- Concurrent Node Departures ---"""
    """node_id1 = "4b12"
    node_id2 = "d3ad"

    leave_thread1 = threading.Thread(target=leave, args=(network, node_id1))
    leave_thread2 = threading.Thread(target=leave, args=(network, node_id2))

    leave_thread1.start()
    leave_thread2.start()

    leave_thread1.join()
    leave_thread2.join()"""

    """--- Concurrent Node Leave and Delete Key from the leaving node ---"""
    """node_id = "c816"
    delete_key = "28ad"

    leave_thread = threading.Thread(target=leave, args=(network, node_id))
    delete_thread = threading.Thread(target=delete_key, args=(node_id, delete_key))

    delete_thread.start()
    leave_thread.start()

    delete_thread.join()
    leave_thread.join()"""

    # Show the DHT GUI
    network.gui.show_dht_gui()
    network.gui.root.mainloop()


if __name__ == "__main__":
    main()
