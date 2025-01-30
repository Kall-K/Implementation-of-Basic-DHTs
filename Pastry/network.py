import numpy as np

from helper_functions import *
from gui import GUI

evenly_spaced_nodes = 16
positions = positions = np.linspace(0, 1, evenly_spaced_nodes)  # Generates evenly spaced points


class PastryNetwork:
    def __init__(self):
        self.nodes = {}  # Dictionary. Keys are node IDs, values are Node objects
        self.node_ports = {}  # Dictionary. Keys are node IDs, values are ports
        self.used_ports = []
        self.used_positions = list(positions)

        self.gui = GUI(self)  # Initialize the GUI
        # Initialize plotting
        # self.setup_plot()

    def node_join(self, new_node):
        """
        Handles a new node joining the Pastry network.
        """
        # Determine the new node's ID
        new_node_id = new_node.node_id

        # Check if the node ID is already in use
        if new_node_id in self.nodes.keys():
            print(f"Node {new_node_id} already exists in the network.")
            return {"status": "failure", "message": f"Node {new_node_id} already exists."}

        if self.used_positions:
            new_node.position = self.used_positions.pop(0)
        else:
            new_node.position = np.random.uniform(0, 1)  # Fallback

        # Add the node object to the network
        self.nodes[new_node_id] = new_node

        # Add the node's port to the node_ports dictionary
        self.node_ports[new_node_id] = new_node.port

        if len(self.nodes) == 1:
            print("The network is empty. The new node is the first node.")
            self.gui.visualize_network()
            self.gui.visualize_topology()
            return

        # Find the closest node to the new using its position
        closest_node_id, closest_neighborhood_set = self._find_topologically_closest_node(new_node)

        # Initialize the new nodes Neighborhood Set of the new node
        print(f"\nInitializing Neighborhood Set of the new node {new_node_id}...")
        new_node.initialize_neighborhood_set(closest_node_id, closest_neighborhood_set)

        # Forward the join message to the topologically closest node
        join_request = {
            "operation": "NODE_JOIN",
            "joining_node_id": new_node_id,
            "hops": [],  # Initialize an empty hops list
        }
        print(f"\nForwarding JOIN_NETWORK request to the closest node {closest_node_id}...")
        response = new_node.send_request(self.node_ports[closest_node_id], join_request)

        # Extract and print the hop count from the response
        if response and "hops" in response:
            hop_count = len(response["hops"])
            print(f"\nHops during node join for {new_node_id}: {hop_count}")
            print(f"Full hops list: {response['hops']}")
        else:
            print(f"Failed to retrieve hop count for {new_node_id}. Response: {response}")

        # Broadcast the new node's arrival to the network
        print(f"\nBroadcasting the new node's arrival to the network...")
        new_node.transmit_state()

        # Move any keys that should be stored in the new node

        # Visualize the network after the new node joins
        self.gui.visualize_network()
        self.gui.visualize_topology()

        return response

    def leave(self, leaving_node_id):
        """
        Handles the leave operation for a node in the network.
        """
        print(f"Network: Processing leave request for Node {leaving_node_id}.")

        # Initialize hops tracking
        hops = []

        # Check if the node exists in the network
        if leaving_node_id not in self.nodes:
            print(f"Network: Node {leaving_node_id} does not exist.")
            return {"status": "failure", "message": f"Node {leaving_node_id} not found.", "hops": hops}

        leaving_node = self.nodes[leaving_node_id]
        keys_to_store = []

        with leaving_node.lock:
            # Extract keys from the KDTree of the leaving node
            if not leaving_node.kd_tree or not leaving_node.kd_tree.points.size:
                print(f"Network: Node {leaving_node_id} has no keys to store.")
            else:
                # Extract points, reviews, and countries from the KDTree
                keys_to_store = [
                    {
                        "key": country_key,
                        "position": point,
                        "review": review,
                        "country": country,
                    }
                    for country_key, point, review, country in zip(
                        leaving_node.kd_tree.country_keys,
                        leaving_node.kd_tree.points,
                        leaving_node.kd_tree.reviews,
                        leaving_node.kd_tree.countries,  # Use the original country
                    )
                ]
                print(f"Network: Stored {len(keys_to_store)} keys from Node {leaving_node_id}.")

        # Remove the leaving node from the network
        print(f"Network: Removing Node {leaving_node_id} from the network.")
        del self.nodes[leaving_node_id]
        del self.node_ports[leaving_node_id]

        # Notify remaining nodes about the departure
        print(f"Network: Notifying affected nodes about Node {leaving_node_id}'s departure.")
        available_nodes = list(self.nodes.keys())
        node_positions = {node_id: self.nodes[node_id].position for node_id in self.nodes}

        for node_id in available_nodes:
            leave_request = {
                "operation": "NODE_LEAVE",
                "leaving_node_id": leaving_node_id,
                "available_nodes": available_nodes,
                "node_positions": node_positions,
                "hops": hops,  # Pass the hops list
            }
            response = self.nodes[node_id].send_request(self.node_ports[node_id], leave_request)

            # Update the hops list from the response
            if response and "hops" in response:
                hops = response["hops"]

        # Check if there are any available nodes for reinsertion
        if not available_nodes:
            print("Network: No available nodes to reinsert keys. Keys will not be reinserted.")
            return {"status": "failure", "message": "No nodes available for reinsertion.", "hops": hops}

        # Reinsert stored keys using the network-level insert_key function
        print(f"Network: Reinserting stored keys into the network.")
        reinserted_count = 0
        for key_data in keys_to_store:
            key = key_data["key"]
            position = key_data["position"]
            review = key_data["review"]
            country = key_data["country"]  # Original country

            # Find the node with the minimum subtraction value from the key
            closest_node_id = min(available_nodes, key=lambda node_id: abs(int(node_id, 16) - int(key, 16)))

            try:
                print(f"Network: Redirecting key {key} (Country: {country}) from Node: {node_id}  to Node {closest_node_id}.")
                self.nodes[closest_node_id].insert_key(key, position, review, country)
                reinserted_count += 1
            except Exception as e:
                print(f"Network: Failed to redirect key {key} to Node {closest_node_id}. Error: {e}")

        skipped_count = len(keys_to_store) - reinserted_count
        print(f"Network: Successfully reinserted {reinserted_count} keys. Skipped {skipped_count} keys.")

        print(f"Network: Node {leaving_node_id} has successfully left the network.")
        print(f"The Hopes after leaving are: {hops}")

        # for node_id, node in self.nodes.items():
        #     print(f"Node {node_id}: KD-Tree Keys: {node.kd_tree.country_keys if node.kd_tree else 'None'}")
        #     print(f"Node {node_id}: Routing Table: {node.routing_table}")

        return {"status": "success", "message": f"Node {leaving_node_id} has left the network.", "hops": hops}

    def _find_topologically_closest_node(self, new_node):
        """
        Find the topologically closest node in the network to the new node.
        """
        closest_node_id = None
        closest_neighborhood_set = None
        min_distance = float("inf")
        for existing_node_id in self.node_ports.keys():
            # Skip the new node
            if existing_node_id == new_node.node_id:
                continue

            dist_request = {
                "operation": "DISTANCE",
                "node_position": new_node.position,
                "hops": [],
            }
            response = new_node.send_request(self.node_ports[existing_node_id], dist_request)

            distance = response["distance"]
            neighborhood_set = response["neighborhood_set"]
            if distance < min_distance:
                closest_node_id = existing_node_id
                closest_neighborhood_set = neighborhood_set
                min_distance = distance
        return closest_node_id, closest_neighborhood_set
