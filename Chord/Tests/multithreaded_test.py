import threading

import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from Chord.network import ChordNetwork
from Chord.node import ChordNode
from constants import *
from helper_functions import hash_key
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
    if N is None:
        response = node.lookup(lookup_key, lower_bounds, upper_bounds)
    else:
        response = node.lookup(lookup_key, lower_bounds, upper_bounds, N)
    # print(f"[Thread {thread_id}] Node {node.node_id} Response: {response}")


def join(network, node_id):
    """Join a node to the network."""
    thread_id = threading.get_ident()
    print(f"\n[Thread {thread_id}] Node {node_id}: Joining the network.\n")
    node = ChordNode(network, node_id)
    node.start_server()
    response = network.node_join(node)
    print(f"[Thread {thread_id}] Network Response: {response}")


def leave(node):
    """Remove a node from the network"""
    thread_id = threading.get_ident()
    print(f"\n[Thread {thread_id}] Node {node.node_id}: Leaving the network gracefully.\n")
    response = node.leave()
    print(f"[Thread {thread_id}] Network Response: {response}")


# Helper functions for user input
def select_operation(thread_num):
    """Prompt user to select an operation for a thread."""
    print(f"\nSelect operation for Thread {thread_num}:")
    print("1. Insert Key")
    print("2. Update Key")
    print("3. Delete Key")
    print("4. Lookup Key")
    print("5. Join Node")
    print("6. Leave Node")
    choice = input("Enter your choice (1-6): ")
    return choice


def get_node(network, prompt="Select a node: "):
    """Prompt user to select a node from the network."""
    print("\nAvailable nodes:")
    for i, node_id in enumerate(network.nodes.keys()):
        print(f"{i+1}. {node_id}")
    while True:
        try:
            choice = int(input(prompt)) - 1
            return list(network.nodes.values())[choice]
        except (ValueError, IndexError):
            print("Invalid selection. Please try again.")


def get_insert_params():
    """Get parameters for insert operation."""
    print("\nEnter paramenters for Key Insertion")
    country = input("Enter country: ")
    name = input("Enter name: ")
    year = int(input("Enter year: "))
    rating = float(input("Enter rating: "))
    price = float(input("Enter price: "))
    review = input("Enter review: ")
    return {
        "key": hash_key(country),
        "point": [year, rating, price],
        "review": review,
        "country": country,
        "name": name,
    }


def get_update_params():
    """Get parameters for update operation."""
    print("\nEnter parameters for Key Update")
    # Collect the key (country) to update
    country = input("\nEnter the country to update: ").strip()
    key = hash_key(country)

    # Collect updated data
    print("\nEnter updated data (leave blank to skip):")
    updated_year = input("Updated year: ").strip()
    updated_rating = input("Updated rating: ").strip()
    updated_price = input("Updated price: ").strip()
    updated_review = input("Updated review: ").strip()

    # Validate that at least one field is provided
    if not (updated_year or updated_rating or updated_price or updated_review):
        print("At least one field must be updated.")
        return None

    # Prepare updated_data dictionary
    updated_data = {}
    if updated_year and updated_rating and updated_price:
        # Use the "point" field if all three fields are provided
        updated_data["point"] = [
            int(updated_year),
            float(updated_rating),
            float(updated_price),
        ]
        if updated_review:
            updated_data["review"] = updated_review
    else:
        # Use the "attributes" field for partial updates
        updated_data["attributes"] = {
            "review_date": int(updated_year) if updated_year else None,
            "rating": float(updated_rating) if updated_rating else None,
            "price": float(updated_price) if updated_price else None,
        }
        # Remove None values from attributes
        updated_data["attributes"] = {
            k: v for k, v in updated_data["attributes"].items() if v is not None
        }
        # Add review if provided
        if updated_review:
            updated_data["review"] = updated_review

    # Collect criteria from the user
    print("\nEnter criteria for the update (leave blank to skip):")
    year_criteria = input("Year criteria: ").strip()
    rating_criteria = input("Rating criteria: ").strip()
    price_criteria = input("Price criteria: ").strip()

    # Prepare criteria dictionary
    criteria = {
        "review_date": int(year_criteria) if year_criteria else None,
        "rating": float(rating_criteria) if rating_criteria else None,
        "price": float(price_criteria) if price_criteria else None,
    }
    # Remove None values from criteria
    criteria = {k: v for k, v in criteria.items() if v is not None}

    return {
        "key": key,
        "updated_data": updated_data,
        "criteria": criteria if criteria else None,
    }


def get_delete_params():
    """Get parameters for delete operation."""
    print("\nEnter paramenters for Key Deletion")
    country = input("Enter country to delete: ")
    return {"key": hash_key(country)}


def get_lookup_params():
    """Get parameters for lookup operation."""
    print("\nEnter paramenters for Key Lookup")
    country = input("Enter country to lookup: ")
    print("Enter lower bounds (comma-separated, use 'None' for null values):")
    lower = [
        x.strip() if x.strip().lower() != "none" else None
        for x in input("Format: year,rating,price (e.g., 2020,None,None): ").split(",")
    ]
    print("Enter upper bounds (comma-separated, use 'None' for null values):")
    upper = [
        x.strip() if x.strip().lower() != "none" else None
        for x in input("Format: year,rating,price (e.g., 2021,None,None): ").split(",")
    ]
    N = input("Enter N (or leave blank for default value): ") or None
    return {
        "lookup_key": hash_key(country),
        "lower_bounds": [
            int(lower[0]) if lower[0] else None,
            float(lower[1]) if lower[1] else None,
            float(lower[2]) if lower[2] else None,
        ],
        "upper_bounds": [
            int(upper[0]) if upper[0] else None,
            float(upper[1]) if upper[1] else None,
            float(upper[2]) if upper[2] else None,
        ],
        "N": int(N) if N else None,
    }


def get_join_params():
    """Get parameters for join operation."""
    print("\nEnter paramenters for Node Join")
    return {"node_id": input("Enter new node ID: ")}


def get_leave_params(network):
    """Get parameters for leave operation."""
    print("\nEnter paramenters for Node Leave")
    return {"node_id": get_node(network, "Select node to leave: ").node_id}


def create_thread(network, operation_choice):
    """Create a thread based on the selected operation."""
    match operation_choice:
        case "1":  # Insert
            params = get_insert_params()
            node = get_node(network)
            return threading.Thread(
                target=insert_key,
                args=(
                    node,
                    params["key"],
                    params["point"],
                    params["review"],
                    params["country"],
                    params["name"],
                ),
            )
        case "2":  # Update
            params = get_update_params()
            node = get_node(network)
            return threading.Thread(
                target=update_key,
                args=(node, params["key"], params["updated_data"], params["criteria"]),
            )
        case "3":  # Delete
            params = get_delete_params()
            node = get_node(network)
            return threading.Thread(target=delete_key, args=(node, params["key"]))
        case "4":  # Lookup
            params = get_lookup_params()
            node = get_node(network)
            return threading.Thread(
                target=lookup_key,
                args=(
                    node,
                    params["lookup_key"],
                    params["lower_bounds"],
                    params["upper_bounds"],
                    params["N"],
                ),
            )
        case "5":  # Join
            params = get_join_params()
            return threading.Thread(target=join, args=(network, params["node_id"]))
        case "6":  # Leave
            params = get_leave_params(network)
            return threading.Thread(target=leave, args=(network, params["node_id"]))
        case _:
            raise ValueError("Invalid operation selected")


def main():
    network = ChordNetwork()
    network.build(
        predefined_ids=predefined_ids,
        dataset_path="../../Coffee_Reviews_Dataset/simplified_coffee.csv",
    )

    # User selects operations for both threads
    thread1_choice = select_operation(1)
    thread1 = create_thread(network, thread1_choice)

    thread2_choice = select_operation(2)
    thread2 = create_thread(network, thread2_choice)

    # Start and join threads
    print("\nStarting concurrent operations...")
    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    print("\nAll operations completed!")

    # Show the DHT GUI
    network.gui.show_dht_gui()
    network.gui.root.mainloop()


if __name__ == "__main__":
    main()
