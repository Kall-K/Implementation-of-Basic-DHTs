import threading
import socket
import hashlib
import pickle
from concurrent.futures import ThreadPoolExecutor
import numpy as np

from constants import *
from helper_functions import *

from Multidimensional_Data_Structures.kd_tree import KDTree
from Multidimensional_Data_Structures.lsh import LSH
from sklearn.feature_extraction.text import TfidfVectorizer


class ChordNode:

    def __init__(self, network, node_id=None):
        """
        Initialize a new Chord node with a unique ID, address, and empty data structures.
        """
        self.address = self._generate_address()  # (IP, Port)
        self.node_id = (
            node_id if node_id is not None else self._generate_id(self.address)
        )
        self.network = network  # Reference to the DHT network
        # self.kd_tree = None  # Centralized KD-Tree
        
        self.successor = self.node_id
        self.predecessor = self.node_id
        self.finger_table = [self.node_id] * M
        self.running = True # maybe useless
        self.kdtree = None

        self.data = {}  # For storing key-value pairs

        # Create a thread pool for handling requests to limit the number of concurrent threads
        self.thread_pool = ThreadPoolExecutor(max_workers=10)

    # Initialization Methods

    def _generate_address(self, port=None):
        """
        Generate a unique address (IP, Port) for the node.
        """
        # Simulate unique IPs in a private network range (192.168.x.x)
        ip = f"192.168.{np.random.randint(0, 256)}.{np.random.randint(1, 256)}"
        port = port or self._generate_port()
        return (ip, port)

    def _generate_id(self, address):
        """
        Generate a unique node ID by hashing the address.
        """
        address_str = f"{address[0]}:{address[1]}"
        sha1_hash = hashlib.sha1(address_str.encode()).hexdigest()
        node_id = sha1_hash[-HASH_HEX_DIGITS:]  # Take the last 128 bits
        return node_id
    
    def _generate_port(self):
        """
        Generate a unique address Port for the node.
        """
        excluded_ranges = self.get_excluded_ports()

        def is_excluded(port):
            return any(start <= port <= end for start, end in excluded_ranges)

        while True:
            port = np.random.randint(1024, 65535)  # Random port if not provided

            if port not in self.network.used_ports and not is_excluded(port):
                self.network.used_ports.append(port)
                return port

    # Stop running

    def stop(self):
        """Stop the node server."""
        self.running = False

    # State Inspection

    def print_state(self):
        """
        Print the state of the node (ID, Address, Data Structures).
        """
        print("\n" + "-" * 100)
        print(f"Node ID: {self.node_id}")
        print(f"Address: {self.address}")
        print(f"Predecessor: {self.predecessor}")
        print("\nFinger Table:")
        print(self.finger_table)
        print("\n" + "-" * 100)
        # for finger in self.finger_table:
        #     print(finger)

    # Network Communication

    def start_server(self):
        """
        Start the server thread to listen for incoming requests.
        """
        server_thread = threading.Thread(target=self._server, daemon=True)
        server_thread.start()

    def _server(self):
        """
        Set up a socket server to handle incoming requests.
        """
        # Use loopback for actual binding
        bind_ip = "127.0.0.1"  # Bind to localhost for real communication
        bind_address = (bind_ip, self.address[1])

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(bind_address)  # Bind to localhost
            except OSError as e:
                print(f"Error binding to {bind_address}: {e}")
                return

            s.listen()
            print(
                f"\nNode {self.node_id} listening on {self.address} (bound to {bind_address})"
            )
            while True:
                conn, addr = s.accept()  # Accept incoming connection
                # Submit the connection to the thread pool for handling
                self.thread_pool.submit(self._handle_request, conn)

    def _handle_request(self, conn):
        try:
            data = conn.recv(1024)  # Read up to 1024 bytes of data
            request = pickle.loads(data)  # Deserialize the request
            operation = request["operation"]
            print(f"Node {self.node_id}: Handling Request: {request}")
            response = None

            if operation == "FIND_SUCCESSOR":
                response = self._handle_find_successor(request)
            if operation == "DELETE_SUCCESSOR_KEYS":
                response = self._handle_delete_successor_keys(request)
            if operation == "SET_SUCCESSOR":
                response = self._handle_set_successor(request)
            if operation == "SET_PREDECESSOR":
                response = self._handle_set_predecessor(request)

            # Add more operations here as needed

            conn.sendall(pickle.dumps(response))  # Serialize and send the response
        except Exception as e:
            print(f"Error handling request: {e}")
        finally:
            conn.close()

    def send_request(self, node, request):
        """
        Send a request to a node and wait for its response.
        """
        # Use loopback IP for actual connection
        connect_address = ("127.0.0.1", node.address[1])

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # s.settimeout(10)  # Set a timeout for both connect and recv
            try:
                s.connect(connect_address)  # Connect using loopback
                s.sendall(pickle.dumps(request))  # Serialize and send the request
                response = s.recv(1024)  # Receive the response
                print("response: " + pickle.loads(response))
            except Exception as e:
                print(f"Error connecting to {connect_address}: {e}")
                return None

        return pickle.loads(response)  # Deserialize the response
    
    #############################
    ######### Requests ##########
    #############################
    
    def request_find_successor(self, key, node):
        get_successor_request = {
            "operation": "FIND_SUCCESSOR",
            "key": key,
        }
        # Get the possition on the ring
        successor_id = self.send_request(node, get_successor_request)
        return successor_id
    
    def request_delete_successor_keys(self, keys, successor_id):
        node = self.network.nodes[successor_id]
        delete_successor_keys = {
            "operation": "DELETE_SUCCESSOR_KEYS",
            "keys": keys,
        }
        status = self.send_request(node, delete_successor_keys)
        return status

    def request_set_successor(self, successor_id, node_id):
        """
        Send a request to the node with node_id to set its successor to successor_id.
        """
        node = self.network.nodes[node_id]
        set_successor = {
            "operation": "SET_SUCCESSOR",
            "successor": successor_id,
        }
        status = self.send_request(node, set_successor)
        return status
    
    def request_set_predecessor(self, predecessor_id, node_id):
        """
        Send a request to the node with node_id to set its successor to predecessor_id.
        """
        node = self.network.nodes[node_id]
        set_predecessor = {
            "operation": "SET_PREDECESSOR",
            "predecessor": predecessor_id,
        }
        status = self.send_request(node, set_predecessor)
        return status

    #############################
    ######### Handlers ##########
    #############################

    def _handle_find_successor(self, request):
        key = request["key"]
        if self.node_id == key:
            return self.node_id
        if self.distance(self.node_id, key) <= self.distance(self.successor, key):
            return self.successor
        else:
            closest_preceding_node_id = self.closest_preceding_node(self, key)
            closest_preceding_node = self.network.nodes[closest_preceding_node_id]
            return self.request_find_successor(key, closest_preceding_node)
        
    def _handle_delete_successor_keys(self, request):
        keys = request["keys"]
        for key in keys:
            del self.data[key]
        return 0

    def _handle_set_successor(self, request):
        successor_id = request["successor"]
        self.finger_table[0] = successor_id
        self.successor = successor_id
        return 0
    
    def _handle_set_predecessor(self, request):
        predecessor_id = request["predecessor"]
        self.predecessor = predecessor_id
        return 0

    def _handle_lookup_request(self, request): # adjust it to chord
        """
        Handle a LOOKUP operation.
        """
        key = request["key"]
        lower_bounds = request["lower_bounds"]
        upper_bounds = request["upper_bounds"]
        N = request["N"]

        # If this key is found in the leaf set or the next hop is the current node the lookup is successful
        next_hop_id = self._find_next_hop(key)

        if self._in_leaf_set(key) or next_hop_id == self.node_id:
            print(f"\nNode {self.node_id}: Lookup Key {key} Found.")

            # If the KDTree is not initialized or has no data, return a failure message
            if not self.kd_tree or self.kd_tree.points.size == 0:
                print(f"Node {self.node_id}: No data for key {key}.")
                return {"status": "failure", "message": f"No data for key {key}."}

            # KDTree Range Search
            points, reviews = self.kd_tree.search(lower_bounds, upper_bounds)
            print(f"Node {self.node_id}: Found {len(points)} matching points.")

            # LSH Similarity Search
            vectorizer = TfidfVectorizer()
            doc_vectors = vectorizer.fit_transform(reviews).toarray()

            lsh = LSH(num_bands=4, num_rows=5)
            for vector in doc_vectors:
                lsh.add_document(vector)

            similar_pairs = lsh.find_similar_pairs(N)
            similar_docs = lsh.find_similar_docs(similar_pairs, reviews, N)

            print(f"\nThe {N} Most Similar Reviews:\n")
            for i, doc in enumerate(similar_docs, 1):
                print(f"{i}. {doc}\n")

            return {
                "status": "success",
                "message": f"Found {len(points)} matching points.",
            }

        # If the key is not found in the leaf set and the next hop is not the current node
        # forward the request to the next hop
        response = self.send_request(self.network.node_ports[next_hop_id], request)
        return response
    
    def _handle_insert_key_request(self, request):
        """
        Handle an INSERT_KEY operation.
        """
        key = request["key"]
        point = request["point"]
        review = request["review"]
        country = request["country"]
        country_key = hash_key(country)
        hops = request.get("hops", [])

        next_hop_id = self._find_next_hop(key)

        # Determine if this node should store the key or forward it
        if self._in_leaf_set(key) or next_hop_id == self.node_id:
            if not self.kd_tree:
                # Initialize KDTree with the first point
                self.kd_tree = KDTree(
                    points=np.array([point]),
                    reviews=np.array([review]),
                    country_keys=np.array([country_key]),
                )
            else:
                # Add point to the existing KDTree
                self.kd_tree.add_point(point, review, country)

            # Print the point and review directly after adding
            print(f"\nInserted Key: {key}")
            print(f"Point: {point}")
            print(f"Review: {review}")
            print(f"Routed and stored at Node ID: {self.node_id}")
            print(f"Hops: {hops}")
            print("")
            return {
                "status": "success",
                "message": f"Key {key} stored at {self.node_id}",
            }

        # If this node is not responsible for the key forward request to the next hop
        return self.send_request(self.network.node_ports[next_hop_id], request)

    def _handle_update_key_request(self, request):
        """
        Handle an UPDATE_KEY operation with criteria and update fields.
        """
        key = request["key"]
        criteria = request.get("criteria", None)  # Optional criteria to filter
        update_fields = request["data"]  # Update fields for the KDTree
        hops = request.get("hops", [])

        # Find the next hop or check if this node is responsible for the key
        next_hop_id = self._find_next_hop(key)

        if self._in_leaf_set(key) or next_hop_id == self.node_id:
            # Check if the key exists in this node's data structure
            if self.kd_tree and key in self.kd_tree.country_keys:
                # Update the data in the KDTree
                self.kd_tree.update_points(
                    country_key=key,
                    criteria=criteria,
                    update_fields=update_fields,
                )
                print(f"Node {self.node_id}: Key {key} updated successfully.")
                return {
                    "status": "success",
                    "message": f"Key {key} updated successfully.",
                    "hops": hops,
                }
            else:
                return {"status": "failure", "message": f"Key {key} not found.", "hops": hops}

        # Forward the request to the next hop if not responsible for the key
        return self.send_request(self.network.node_ports[next_hop_id], request)
    
    def _handle_delete_key_request(self, request):
        """
        Handle a DELETE_KEY operation.
        """
        key = request["key"]

        next_hop_id = self._find_next_hop(key)

        # If the key belongs to this node (based on leaf set), delete it from the KDTree
        if self._in_leaf_set(key) or next_hop_id == self.node_id:
            if not self.kd_tree:
                print(f"\nNode {self.node_id}: No data for key {key}.")
                return {"status": "failure", "message": f"No data for key {key}.\n"}

            # Delete the key from the KDTree if it exists
            if key in self.kd_tree.country_keys:
                print(f"\nNode {self.node_id}: Deleted Key {key}.")
                self.kd_tree.delete_points(key)
            else:
                print(f"\nNode {self.node_id}: No data for key {key}.\n")
                return {"status": "failure", "message": f"No data for key {key}."}

            return {"status": "success", "message": f"Deleted Key {key}."}

        # Otherwise, forward the request to the next node
        response = self.send_request(self.network.node_ports[next_hop_id], request)
        return response
    #############################
    #### Update Finger Table ####
    #############################
    
    # Ανανεώνει τα fingers του κόμβου
    def update_finger_table(self, node_left = None, leave = False):
        for i in range(1, len(self.finger_table)):
            temp_node = self.request_find_successor((int(self.node_id, 16) + 2 ** i) % R, self)
            # if leave:
            #     if node_left != temp_node:
            #         self.finger_table[i] = temp_node
            #     else: 
            #         self.finger_table[i] = self.request_find_successor((temp_node + 2 ** i) % R)
            # else:
            self.finger_table[i] = temp_node

    #############################
    ###### Find Successor #######
    #############################
        
    # Βρίσκει τον κόμβο που είναι πιο κοντά στο key
    def closest_preceding_node(self, node, h_key):
        for i in range(len(node.finger_table)-1, 0, -1):
            if self.distance(node.finger_table[i-1], h_key) < self.distance(node.finger_table[i], h_key):
                return node.finger_table[i-1]

        return node.finger_table[-1]
        
    def distance(self, hex1, hex2):
        [n1, n2] = [int(str(hex1), 16), int(str(hex2), 16)]
        if n1 <= n2: return n2 - n1
        else: return R - n1 + n2

    #############################
    ######## Node Join ##########
    #############################

    def join(self, successor_node):
        suc_id = successor_node.node_id
        pre_id = successor_node.predecessor
        
        self.find_node_place(pre_id, suc_id)
        self.update_finger_table()

        # Get keys from successor
        self.data = {key: successor_node.data[key] for key in sorted(
            successor_node.data.keys()) if key <= self.node_id}

        keys_to_delete_from_successor = []
        for key in sorted(self.data.keys()):
            if key in successor_node.data:
                keys_to_delete_from_successor.append(key)
        
        # Delete keys
        self.request_delete_successor_keys(keys_to_delete_from_successor, suc_id)


    # Βρίσκει τη θέση του κόμβου
    def find_node_place(self, pre_id, suc_id):
        self.request_set_successor(self.node_id, pre_id) # pre.finger_table[0] = self.node_id, pre.successor = self.node_id
        self.request_set_predecessor(self.node_id, suc_id) #suc.predecessor = self.node_id
        self.finger_table[0] = suc_id
        self.successor = suc_id
        self.predecessor = pre_id


    #############################
    ########### DATA ############
    #############################

    # Data Structure Updates
    def update_routing_table(self, row_idx, received_row):
        """
        Update the routing table of the current node with the received row.
        """
        for col_idx in range(len(received_row)):
            entry = received_row[col_idx]
            if entry is None:
                continue
            # Skip if the entry's hex digit at row_idx matches this node's ID at the same index.
            # This avoids conflicts in the routing table.
            if entry[row_idx] == self.node_id[row_idx]:
                continue
            # Update the routing table with the received entry if the current entry is empty
            if self.routing_table[row_idx][col_idx] is None:
                self.routing_table[row_idx][col_idx] = received_row[col_idx]


    def _update_presence(self, key):
        """
        Update the presence of a node in all the data structures of this node.
        """
        # Neighborhood Set (M)
        if key not in self.neighborhood_set:
            self._update_neighborhood_set(key)

        # Routing Table (R)
        # Find the length of the common prefix between the key and the current node's ID
        idx = common_prefix_length(key, self.node_id)

        # If the entry in the routing table is empty, update it with the key
        if self.routing_table[idx][int(key[idx], 16)] is None:
            self.routing_table[idx][int(key[idx], 16)] = key

        """Giati to ekana auto!! na to psaksw an xreiazetai"""
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
        # If key >= this node's ID, update Lmax
        if hex_compare(key, self.node_id):
            if key not in self.Lmax:
                self._update_leaf_list(self.Lmax, key)
        # Else update Lmin
        else:
            if key not in self.Lmin:
                self._update_leaf_list(self.Lmin, key)

    # Helper Methods


   