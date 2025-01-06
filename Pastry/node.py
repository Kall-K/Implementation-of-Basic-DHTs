import threading
import socket
import hashlib
import pickle
import numpy as np

from constants import *
from helper_functions import *
from Multidimensional_Data_Structures.kd_tree import KDTree
from Multidimensional_Data_Structures.lsh import LSH
from sklearn.feature_extraction.text import TfidfVectorizer


class PastryNode:

    def __init__(self, network):
        """
        Initialize a new Pastry node with a unique ID, address, and empty data structures.
        """
        self.address = self._generate_address()  # (IP, Port)
        self.node_id = self._generate_id(self.address)  # Unique node ID
        self.network = network  # Reference to the DHT network
        self.kd_tree = None  # Centralized KD-Tree
        # 2D Routing Table
        self.routing_table = [
            [None for j in range(pow(2, b))] for i in range(HASH_HEX_DIGITS)
        ]
        # Leaf Set
        self.Lmin = [None for x in range(pow(2, b - 1))]
        self.Lmax = [None for x in range(pow(2, b - 1))]
        self.neighborhood_set = [None for x in range(pow(2, b + 1))]  # Nearby nodes
        self.lock = threading.Lock()

    def _generate_address(self, port=None):
        """
        Generate a random address (IP, Port) for the node.
        """
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        port = port or np.random.randint(1024, 65535)  # Random port if not provided
        return (local_ip, port)

    def _generate_id(self, address):
        """
        Generate a unique node ID by hashing the address.
        """
        address_str = f"{address[0]}:{address[1]}"
        sha1_hash = hashlib.sha1(address_str.encode()).hexdigest()
        node_id = sha1_hash[-HASH_HEX_DIGITS]  # Take the last 128 bits
        return node_id

    def start_server(self):
        """
        Start the server thread to listen for incoming requests.
        """
        server_thread = threading.Thread(target=self._server)
        server_thread.start()

    def _server(self):
        """
        Set up a socket server to handle incoming requests.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(self.address)
            s.listen()
            print(f"Node {self.node_id} listening on {self.address}")
            while True:
                conn, addr = s.accept()  # Accept incoming connection
                threading.Thread(target=self._handle_request, args=(conn,)).start()

    def update_neighborhood_set(self, key):
        """
        Update the neighborhood set with the key.
        """
        self.neighborhood_set = key.neighborhood_set.copy()

        # Insert the key if there is space
        for i in range(len(self.neighborhood_set)):
            if self.neighborhood_set[i] is None:
                self.neighborhood_set[i] = key.node_id
                return

        # If there is no space, replace the farthest node id with the key
        max_dist = -1
        idx = -1
        for i in range(len(self.M)):
            dist = topological_distance(self.address[0], key.address[0])
            if dist > max_dist:
                max_dist = dist
                idx = i
        self.neighborhood_set[idx] = key.node_id

    """def update_routing_table(self, new_node_id):
        
        # Update the routing table with a new node's information.
        
        common_prefix_length = self._common_prefix_length(self.node_id, new_node_id)
        next_digit = int(new_node_id[common_prefix_length], 16)  # Convert hex to int
        self.routing_table[common_prefix_length, next_digit] = new_node_id"""

    def _handle_request(self, conn):
        """
        Handle incoming requests from other nodes.
        """
        data = conn.recv(1024)  # Read up to 1024 bytes of data
        request = pickle.loads(data)  # Deserialize the request
        operation = request["operation"]
        response = None

        if operation == "JOIN_NETWORK":
            response = self._handle_join_request(request)

        conn.sendall(pickle.dumps(response))  # Serialize and send the response
        conn.close()

    def _handle_join_request(self, request):
        """
        Handle a request from a new node to join the network.
        """
        new_node = request["joining_node"]

        # Determine the routing table row to update
        i = common_prefix_length(self.node_id, new_node.node_id)

        # Update the new node's Routing Table row
        self.network.nodes[new_node.node_id].update_routing_table(
            i, self.routing_table[i]
        )

        self._forward_request(request)

    def _forward_request(self, request):
        """
        Forward a request to the next node in the route.
        """
        operation = request["operation"]
        if operation == "JOIN_NETWORK":
            new_node = request["joining_node"]
            next_hop_id = self._find_next_hop(new_node.node_id)

            if next_hop_id == self.node_id:
                # If the next hop is the current node, update the new node's Leaf Set
                self.network.nodes[new_node.node_id].update_leaf_set(
                    self.Lmin, self.Lmax, self.node_id
                )
            else:
                # Else forward the request to the next hop
                next_hop_node = self.network.nodes[next_hop_id]
                self.send_request(next_hop_node, request)

    def _find_next_hop(self, key):
        """
        Find the next hop to forward a request based on the node ID.
        """
        # Check if the key is in the leaf set
        if self._in_leaf_set(key):
            # If the node_id is in the leaf set
            closest_leaf_id = self._find_closest_leaf_id(key)
            return closest_leaf_id

        # If the key is not in the leaf set, check the routing table
        else:
            i = common_prefix_length(self.node_id, key)
            next_hop = self.routing_table[i][int(key[i], 16)]

            if next_hop is not None:
                return next_hop
            # If the routing table entry is empty,
            # scan all the nodes in the network
            else:
                next_hop = self._find_closest_node_id_all(key)
                return next_hop

    @staticmethod
    def __is_closer_node__(target_node, key, l, curr_node_id):
        """
        Custom condition to compare nodes based on topological and numerical closeness.
        """
        i = common_prefix_length(target_node, key)
        target_key_dist = hex_distance(target_node, key)
        curr_node_key_dist = hex_distance(curr_node_id, key)

        if (i >= l) and (target_key_dist < curr_node_key_dist):
            return True
        else:
            return False

    def _find_closest_node_id_all(self, key):
        """
        Scan all the nodes in the network to find the closest node to the given node ID.
        """
        i = common_prefix_length(self.node_id, key)

        # Check Lmin
        for idx in range(len(self.Lmin)):
            if self.Lmin[idx] is not None:
                if self.__is_closer_node__(self.Lmin[idx], key, idx, self.node_id):
                    return self.Lmin[idx]

        # Check Lmax
        for idx in range(len(self.Lmax)):
            if self.Lmax[idx] is not None:
                if self.__is_closer_node__(self.Lmax[idx], key, idx, self.node_id):
                    return self.Lmax[idx]

        # Check neighborhood set (M)
        for idx in range(len(self.neighborhood_set)):
            if self.neighborhood_set[idx] is not None:
                if self.__is_closer_node__(
                    self.neighborhood_set[idx], key, idx, self.node_id
                ):
                    return self.neighborhood_set[idx]

        # Check routing table (R)
        for row in range(len(self.routing_table)):
            for col in range(len(self.routing_table[0])):
                if self.routing_table[row][col] is not None:
                    if self.__is_closer_node__(
                        self.routing_table[row][col], key, row, self.node_id
                    ):
                        return self.routing_table[row][col]

        # If no node is found, return the current node ID
        return self.node_id

    def _find_closest_leaf_id(self, key):
        common_prefix_length = common_prefix_length(self.node_id, key)
        hex_distance = hex_distance(self.node_id, key)

        closest_leaf_id = self.node_id

        # Check Lmin for closer nodes
        for leaf in self.Lmin:
            if leaf is not None:
                prefix_length = common_prefix_length(leaf, key)
                distance = hex_distance(leaf, key)

                # Update if the prefix length is longer or if the distances are equal but this node is numerically closer
                if (prefix_length > closest_prefix_length) or (
                    prefix_length == closest_prefix_length
                    and distance < closest_distance
                ):
                    closest_leaf_id = leaf
                    closest_prefix_length = prefix_length
                    closest_distance = distance

        # Check Lmax for closer nodes
        for leaf in self.Lmax:
            if leaf is not None:
                prefix_length = common_prefix_length(leaf, key)
                distance = hex_distance(leaf, key)

                # Apply the same update logic
                if (prefix_length > closest_prefix_length) or (
                    prefix_length == closest_prefix_length
                    and distance < closest_distance
                ):
                    closest_leaf_id = leaf
                    closest_prefix_length = prefix_length
                    closest_distance = distance

        return closest_leaf_id

    def _in_leaf_set(self, node_id):
        """
        Check if a node ID is in the leaf set.
        """
        if node_id in self.Lmin or node_id in self.Lmax:
            return True
        else:
            return False

    def update_leaf_set(self, Lmin, Lmax, key):
        """
        Update the leaf set of the current node based on the provided Lmin, Lmax
        and key of the node that triggered the update.
        """
        # Copy the provided leaf sets
        self.Lmin = Lmin.copy()
        self.Lmax = Lmax.copy()

        # Check if the node_id belongs to the Lmax range
        # (greater than or equal to the current node ID)
        if hex_compare(key, self.node_id):
            # Try to insert the node_id into Lmax
            for i in range(len(self.Lmax)):
                if self.Lmax[i] is None:
                    self.Lmax[i] = key
                    return

            # If Lmax is full, replace the farthest node if the key is closer
            max_top_dist = -1
            max_num_dist = -1
            replace_index = -1

            for i in range(len(self.Lmax)):
                top_dist = topological_distance(
                    self.network.nodes[self.Lmax[i]].address[0], self.address[0]
                )
                num_dist = hex_distance(self.Lmax[i], self.node_id)
                if (top_dist > max_top_dist) or (
                    top_dist == max_top_dist and num_dist < max_num_dist
                ):
                    max_top_dist = top_dist
                    max_num_dist = num_dist
                    replace_index = i

            top_dist = topological_distance(
                self.network.nodes[key].address[0], self.address[0]
            )
            num_dist = hex_distance(key, self.node_id)
            if (top_dist > max_top_dist) or (
                top_dist == max_top_dist and num_dist < max_num_dist
            ):
                self.Lmax[replace_index] = key

        else:
            # Try to insert the key into Lmin
            for i in range(len(self.Lmin)):
                if self.Lmin[i] is None:
                    self.Lmin[i] = key
                    return

            # If Lmin is full, replace the farthest node if the key is closer
            max_top_dist = -1
            max_num_dist = -1
            replace_index = -1

            for i in range(len(self.Lmin)):
                top_dist = topological_distance(
                    self.network.nodes[self.Lmin[i]].address[0], self.address[0]
                )
                num_dist = hex_distance(self.Lmin[i], self.node_id)
                if (top_dist > max_top_dist) or (
                    top_dist == max_top_dist and num_dist < max_num_dist
                ):
                    max_top_dist = top_dist
                    max_num_dist = num_dist
                    replace_index = i

            top_dist = topological_distance(
                self.network.nodes[key].address[0], self.address[0]
            )
            num_dist = hex_distance(key, self.node_id)
            if (top_dist > max_top_dist) or (
                top_dist == max_top_dist and num_dist < max_num_dist
            ):
                self.Lmin[replace_index] = key

    def send_request(self, node, request):
        """
        Send a request to a node and wait for its response.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(node.address)
            s.sendall(pickle.dumps(request))  # Serialize and send the request
            response = s.recv(1024)  # Receive the response
        return pickle.loads(response)  # Deserialize the response

    def transmit_state(self):
        """
        Broadcast the arrival of this node to the network.
        """
        node_id = self.node_id

        # Update the Neighborhood Set (M) nodes
        for i in range(len(self.neighborhood_set)):
            if self.neighborhood_set[i] is not None:
                self.network.nodes[self.neighborhood_set[i]].update_presence(node_id)

        # Update the Routing Table (R) nodes
        for i in range(len(self.routing_table)):
            for j in range(len(self.routing_table[0])):
                if self.routing_table[i][j] is not None:
                    self.network.nodes[self.routing_table[i][j]].update_presence(
                        node_id
                    )

        # Update the Leaf Set (L) Nodes

        # Iterate through the Lmin list
        for i in range(len(self.Lmin)):
            # Check if the current entry in the Lmin list is not None
            if self.Lmin[i] is not None:
                # Update the presence of the node in the network
                self.network.nodes[self.Lmin[i]].update_presence(node_id)

        # Iterate through the Lmax list
        for i in range(len(self.Lmax)):
            # Check if the current entry in the Lmax list is not None
            if self.Lmax[i] is not None:
                # Update the presence of the node in the network
                self.network.nodes[self.Lmax[i]].update_presence(node_id)

    def update_presence(self, key):
        """
        Update the presence of a node in all the data structures of this node.
        """
        # Neighborhood Set (M)
        if key not in self.neighborhood_set:
            self.update_neighborhood_set(key)

        # Routing Table (R)
        # Find the length of the common prefix between the key and the current node's ID
        idx = common_prefix_length(key, self.node_id)

        # If the entry in the routing table is empty, update it with the key
        if self.routing_table[idx][int(key[idx], 16)] is None:
            self.routing_table[idx][int(key[idx], 16)] = key

        # If the entry in the routing table of the node corresponding to the key
        # is empty, update it with the current node's ID
        if (
            self.network.nodes[key].routing_table[idx][int(self.node_id[idx], 16)]
            is None
        ):
            self.network.nodes[key].routing_table[idx][
                int(self.node_id[idx], 16)
            ] = self.node_id

        # Leaf Set (Lmin, Lmax)
        if hex_compare(key, self.node_id):
            if key not in self.Lmax:
                self.__update__Lmax(key)
        else:
            if key not in self.Lmin:
                self.__update__Lmin(key)

    def __update__Lmax(self, key):
        # Iterate through the Lmax list to find an empty slot
        for i in range(len(self.Lmax)):
            if self.Lmax[i] is None:
                self.Lmax[i] = key
                return

        # If there are no empty slots, find the farthest node
        # Initialize variables to track the farthest node
        max_top_dist = -1
        max_num_dist = -1
        j = -1

        # Iterate through the Lmax list to find the farthest node
        for i in range(len(self.Lmax)):
            top_dist = topological_distance(
                self.network.nodes[self.Lmax[i]].address[0], self.node_address[0]
            )
            num_dist = hex_distance(self.Lmax[i], self.node_id)

            if (top_dist > max_top_dist) or (
                top_dist == max_top_dist and num_dist < max_num_dist
            ):
                max_top_dist = top_dist
                max_num_dist = num_dist
                j = i

        # Calculate the distance of the new key
        top_dist = topological_distance(
            self.network.nodes[key].address[0], self.node_address[0]
        )
        num_dist = hex_distance(key, self.node_id)

        # If the new key is farther than the current farthest node, replace it
        if (top_dist > max_top_dist) or (
            top_dist == max_top_dist and num_dist < max_num_dist
        ):
            self.Lmax[j] = key

    def __update__Lmin(self, key):
        # Iterate through the Lmin list to find an empty slot
        for i in range(len(self.Lmin)):
            if self.Lmin[i] is None:
                self.Lmin[i] = key
                return

        # Initialize variables to track the farthest node
        max_top_dist = -1
        max_num_dist = -1
        j = -1

        # Iterate through the Lmin list to find the farthest node
        for i in range(len(self.Lmin)):
            top_dist = topological_distance(
                self.network.nodes[self.Lmin[i]].address[0], self.node_address[0]
            )
            num_dist = hex_distance(self.Lmin[i], self.node_id)

            if (top_dist > max_top_dist) or (
                top_dist == max_top_dist and num_dist < max_num_dist
            ):
                max_top_dist = top_dist
                max_num_dist = num_dist
                j = i

        # Calculate the distance of the new key
        top_dist = topological_distance(
            self.network.nodes[key].address[0], self.node_address[0]
        )
        num_dist = hex_distance(key, self.node_id)

        # If the new key is farther than the current farthest node, replace it
        if (top_dist > max_top_dist) or (
            top_dist == max_top_dist and num_dist < max_num_dist
        ):
            self.Lmin[j] = key
