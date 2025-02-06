import time
import pandas as pd
import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


from Pastry.network import PastryNetwork
from Pastry.node import PastryNode
from constants import *
from helper_functions import *


def main():
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

    # Stage 1: Node Joining
    print("Stage 1: Node Joining")
    print("=======================")
    print("Creating the Pastry network...")
    network = PastryNetwork()

    print(f"Adding {len(predefined_ids)} nodes to the network...")
    for node_id in predefined_ids:
        node = PastryNode(network, node_id=node_id)
        node.start_server()
        # time.sleep(0.1)  # Allow the server to start
        network.node_join(node)
        print(f"Node Added: ID = {node.node_id}, Position = {node.position}")
    print("\nAll nodes have successfully joined the network.\n")

    hops_counts = {
        "NODE_JOIN": [],
        "INSERT_KEY": [],
        "LOOKUP": [],
        "DELETE_KEY": [],
        "UPDATE_KEY": [],
        "NODE_LEAVE": [],
    }

    # Track hops for NODE_JOIN
    new_node_id = "d3ad"
    new_node = PastryNode(network, node_id=new_node_id)
    new_node.start_server()
    time.sleep(0.1)  # Allow the server to start
    response = network.node_join(new_node)

    if response and "hops" in response:
        hops_counts["NODE_JOIN"].append(len(response["hops"]))
        print(f"Hops during NODE_JOIN for {new_node_id}: {len(response['hops'])}")
    else:
        print(f"Failed to retrieve hops for NODE_JOIN {new_node_id}.")

    # Stage 2: Key Insertions

    print("Stage 2: Key Insertions")
    print("=======================")
    print("\nInserting data into the network...")
    first_node = list(network.nodes.values())[0]

    # For similarity search testing later insert a custom entry in the USA
    country = "United States"
    name = "Greg's Coffee"
    key = hash_key(country)
    point = [2018, 94, 5.5]
    review = "Very delicate and sweet. Lemon verbena, dried persimmon, dogwood, baker's chocolate in aroma and cup. Balanced, sweet-savory structure; velvety-smooth mouthfeel. The sweetly herb-toned finish centers on notes of lemon verbena and dried persimmon wrapped in baker's chocolate."
    print(f"\nInserting Key: {key}, Country: {country}, Name: {name}\n")

    insert_response = first_node.insert_key(key, point, review, country)
    if insert_response and "hops" in insert_response:
        hops_counts["INSERT_KEY"].append(len(insert_response["hops"]))
        print(f"Hops during INSERT_KEY for {country}: {len(insert_response['hops'])}")
    else:
        print(f"Failed to retrieve hops for INSERT_KEY {country}.")

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
    lookup_response = first_node.lookup(lookup_key, lower_bounds, upper_bounds, N=5)

    if lookup_response and "hops" in lookup_response:
        hops_counts["LOOKUP"].append(len(lookup_response["hops"]))
        print(f"Hops during LOOKUP for {lookup_key}: {len(lookup_response['hops'])}")
    else:
        print(f"Failed to retrieve hops for LOOKUP {lookup_key}.")

    # Stage 4: Key Deletion

    print("\nStage 4: Key Deletion")
    print("=======================")
    delete_key = "372b"
    delete_response = first_node.delete_key(delete_key)

    if delete_response and "hops" in delete_response:
        hops_counts["DELETE_KEY"].append(len(delete_response["hops"]))
        print(f"Hops during DELETE_KEY for {delete_key}: {len(delete_response['hops'])}")
    else:
        print(f"Failed to retrieve hops for DELETE_KEY {delete_key}.")

    first_node.delete_key("aaaa")  # Delete a key that does not exist

    # Lookup the United States key again to see if it was deleted
    # first_node.lookup(lookup_key, lower_bounds, upper_bounds, N=5)

    print("\nStage 5:Key Update")
    print("=======================")

    taiwan_country_key = hash_key("Taiwan")

    # Update all points for Taiwan
    print("\nUpdating all points for Taiwan:\n")
    update_to = {"attributes": {"price": 35.0}}
    first_node.update_key(key=taiwan_country_key, updated_data=update_to)

    # Update a specific point for Taiwan
    print("\nUpdating a specific point for Taiwan:\n")
    update_feilds_that_have = {"review_date": 2019, "rating": 94, "price": 35.0}
    update_to = {"attributes": {"price": 36.0}}
    first_node.update_key(
        key=taiwan_country_key, updated_data=update_to, criteria=update_feilds_that_have
    )

    # Update only the review for Taiwan
    print("\nUpdating only the review for Taiwan:\n")
    update_to = {
        "review": "An updated review for Taiwan's coffee: crisp and fruity with a lingering sweetness."
    }

    update_response = first_node.update_key(taiwan_country_key, updated_data=update_to)
    if update_response and "hops" in update_response:
        hops_counts["UPDATE_KEY"].append(len(update_response["hops"]))
        print(f"Hops during UPDATE_KEY for {taiwan_country_key}: {len(update_response['hops'])}")
    else:
        print(f"Failed to retrieve hops for UPDATE_KEY {taiwan_country_key}.")

    # Update based on specific attributes and modify multiple fields
    print("\nUpdating specific attributes for Taiwan:\n")
    update_feilds_that_have = {"review_date": 2019, "rating": 94}
    update_to = {"point": [2018, 93, None]}
    first_node.update_key(
        key=taiwan_country_key, updated_data=update_to, criteria=update_feilds_that_have
    )

    # Verify all updates
    lower_bounds = [2018, 92, 33.0]
    upper_bounds = [2019, 95, 37.0]
    print("\nVerifying updates through lookup:\n")
    response = first_node.lookup(taiwan_country_key, lower_bounds, upper_bounds, N=5)
    print(response)

    print("\nStage 6: Node Leave")
    print("=======================")
    # Trigger a node leave operation using the network

    node_to_leave = "3722"
    leave_response = network.leave(node_to_leave)

    if leave_response and "hops" in leave_response:
        hops_counts["NODE_LEAVE"].append(len(leave_response["hops"]))
        print(f"Hops during NODE_LEAVE for {node_to_leave}: {len(leave_response['hops'])}")
    else:
        print(f"Failed to retrieve hops for NODE_LEAVE {node_to_leave}.")

    print("\nStage 7: Unexpected Node Failure")
    print("=======================")

    # Choose a random node to fail unexpectedly
    unexpected_failure_node = "4b12"  # Replace with any node ID
    print(f"\nSimulating unexpected failure of Node {unexpected_failure_node}...\n")

    # Trigger unexpected failure
    unexpected_leave_response = network.leave_unexpected(unexpected_failure_node)

    # Print available nodes
    available_nodes = list(network.nodes.keys())
    print(f"Available nodes now: {available_nodes}")

    # If the failed node was the first node, update first_node
    if unexpected_failure_node == first_node.node_id:
        if available_nodes:  # Ensure there are remaining nodes
            first_node = list(network.nodes.values())[0]  # Set to the first available node
            print(f"Updated first_node to: {first_node.node_id}")
        else:
            first_node = None  # No nodes left in the network
            print("No nodes left in the network.")

    if unexpected_leave_response["status"] == "success":
        print(f"Node {unexpected_failure_node} has failed unexpectedly.")
    else:
        print(f"Failed to simulate unexpected failure for Node {unexpected_failure_node}.")

    # # Uncomment to test Insert after node departured unexpectedly : use node 20bd above
    # print("\nAttempting to INSERT a key after unexpected failure...\n")
    # country = "Germany"
    # key = hash_key(country)
    # print(f"key is: {key}")
    # point = [2020, 85, 3.5]
    # review = "Testing repair after unexpected failure."
    # insert_response = first_node.insert_key(key, point, review, country)

    # if insert_response and "hops" in insert_response:
    #     print(f"Hops during INSERT_KEY after unexpected failure: {len(insert_response['hops'])}")
    #     print(f"Full Hops List: {insert_response['hops']}")
    # else:
    #     print("INSERT_KEY Operation failed after unexpected failure.")

    # # Verify repair by performing a LOOKUP
    # print(f"\nAttempting to LOOKUP the inserted key {key} after unexpected failure...\n")
    # lookup_response = first_node.lookup(key, lower_bounds=[2019, 80, 3.0], upper_bounds=[2021, 90, 4.0],N=1)

    # if lookup_response and "hops" in lookup_response:
    #     print(f"Hops during LOOKUP after unexpected failure: {len(lookup_response['hops'])}")
    #     print(f"Full Hops List: {lookup_response['hops']}")
    # else:
    #     print("LOOKUP Operation failed after unexpected failure.")

    # # # # # # # # # #  # # # # #  # # # # # # # # # # # # # # # # # #

    # # Uncomment to test Delete after node departured unexpectedly : use node 4b12 above
    # country_to_delete = "Hawai'i"
    # key_to_delete = hash_key(country_to_delete)
    # print(f"\nDeleting key associated with country: {country_to_delete} (Key: {key_to_delete})\n")

    # # Step 3: Perform DELETE_KEY operation
    # delete_response = first_node.delete_key(key_to_delete)

    # # Step 4: Check if the key was successfully deleted
    # if delete_response and "hops" in delete_response:
    #     print(f"Hops during DELETE_KEY after unexpected failure: {len(delete_response['hops'])}")
    #     print(f"Full Hops List: {delete_response['hops']}")
    #     print("DELETE_KEY Operation completed successfully.")
    # else:
    #     print("DELETE_KEY Operation failed after unexpected failure.")

    # # Step 5: Verify repair by performing a LOOKUP on "Australia"
    # print(f"\nAttempting to LOOKUP the deleted key {key_to_delete} after unexpected failure...\n")
    # lookup_response = first_node.lookup(key_to_delete, lower_bounds=[2019, 80, 3.0], upper_bounds=[2021, 90, 4.0], N=1)

    # if lookup_response and "hops" in lookup_response:
    #     print(f"Hops during LOOKUP after DELETE_KEY: {len(lookup_response['hops'])}")
    #     print(f"Full Hops List: {lookup_response['hops']}")
    #     if lookup_response.get("reviews_len", 0) == 0:
    #         print("LOOKUP confirmed: The key was successfully deleted!")
    #     else:
    #         print("LOOKUP failed: The key was NOT deleted correctly.")
    # else:
    #     print("LOOKUP Operation failed after DELETE_KEY.")

    # # # # # # # # # #  # # # # #  # # # # # # # # # # # # # # # # # #

    # # Uncomment to test UPDATE_KEY after a node departs unexpectedly : use node 20bc above
    # country_to_update = "Canada"
    # key_to_update = hash_key(country_to_update)

    # print(f"\nUpdating key associated with country: {country_to_update} (Key: {key_to_update})\n")

    # # Step 3: Perform UPDATE_KEY operation based on criteria
    # update_feilds_that_have = {"review_date": 2020, "rating": 94}  # Example criteria for matching entries
    # update_to = {"review": "Updated review after unexpected failure."}  # New data to apply

    # update_response = first_node.update_key(key=key_to_update, updated_data=update_to, criteria=update_feilds_that_have)

    # # Step 4: Check if the key was successfully updated
    # if update_response and "hops" in update_response:
    #     print(f"Hops during UPDATE_KEY after unexpected failure: {len(update_response['hops'])}")
    #     print(f"Full Hops List: {update_response['hops']}")
    #     print("UPDATE_KEY Operation completed successfully.")
    # else:
    #     print("UPDATE_KEY Operation failed after unexpected failure.")

    # # Step 5: Verify repair by performing a LOOKUP
    # lower_bounds = [2019, 90, 5.0]
    # upper_bounds = [2021, 97, 7.0]

    # print(f"\nAttempting to LOOKUP the updated key {key_to_update} after unexpected failure...\n")
    # lookup_response = first_node.lookup(key_to_update, lower_bounds, upper_bounds, N=5)

    # if lookup_response and "hops" in lookup_response:
    #     print(f"Hops during LOOKUP after UPDATE_KEY: {len(lookup_response['hops'])}")
    #     print(f"Full Hops List: {lookup_response['hops']}")
    # else:
    #     print("LOOKUP Operation failed after UPDATE_KEY.")

    # # # # # # # # # #  # # # # #  # # # # # # # # # # # # # # # # # #

    # # Choose a key to look up after failure use node 4b12 above
    # country_to_lookup = "Taiwan"
    # key_to_lookup = hash_key(country_to_lookup)

    # # Step 3: Perform LOOKUP operation
    # lower_bounds = [2018, 90, 30.0]
    # upper_bounds = [2020, 100, 40.0]

    # print(f"\nAttempting to LOOKUP key {key_to_lookup} after unexpected failure of {unexpected_failure_node}...\n")
    # lookup_response = first_node.lookup(key_to_lookup, lower_bounds, upper_bounds, N=5)

    # # Step 4: Check if the key was successfully found
    # if lookup_response and "hops" in lookup_response:
    #     print(f"Hops during LOOKUP after unexpected failure: {len(lookup_response['hops'])}")
    #     print(f"Full Hops List: {lookup_response['hops']}")

    #     if lookup_response.get("reviews_len", 0) > 0:
    #         print("LOOKUP confirmed: The key was successfully found after failure!")
    #     else:
    #         print("LOOKUP failed: The key was NOT found after failure.")
    # else:
    #     print("LOOKUP Operation failed after unexpected failure.")

    # Track hops for NODE_JOIN
    new_node_id = "6ad2"
    new_node = PastryNode(network, node_id=new_node_id)
    new_node.start_server()
    time.sleep(0.1)  # Allow the server to start
    response = network.node_join(new_node)

    # Verify the state of the network after the node leaves
    print("\nInspecting the state of the network after the node leaves:")
    for node in network.nodes.values():
        node.print_state()

    plot_hops(hops_counts)
    print(hops_counts)

    # Run the gui main loop
    network.gui.root.mainloop()


if __name__ == "__main__":
    main()
