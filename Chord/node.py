import threading
import socket
import hashlib
import pickle
from concurrent.futures import ThreadPoolExecutor
import time
import numpy as np
import subprocess  # for running netsh to get excluded ports on Windows
import re
import platform  # for system identification to get excluded ports
from constants import *
from helper_functions import *
from collections import defaultdict
import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Multidimensional_Data_Structures.kd_tree import KDTree
from Multidimensional_Data_Structures.lsh import LSH
from sklearn.feature_extraction.text import TfidfVectorizer

class ChordNode:

    def __init__(self, network, node_id=None):
        """
        Initialize a new Chord node with a unique ID, address, and empty data structures.
        """
        self.network = network  # Reference to the DHT network
        self.address = self._generate_address()  # (IP, Port)
        self.node_id = (
            node_id if node_id is not None else self._generate_id(self.address)
        )
        self.kd_tree = None  # Centralized KD-Tree
        
        # self.successor = self.node_id
        self.predecessor = self.node_id
        self.finger_table = [self.node_id] * M

        self.successors = [self.node_id] * S
        self.running = True

        self.data = {}  # For storing key-value pairs

        self.lock = threading.Lock()  # Lock for thread safety

        # Create a thread pool for handling requests to limit the number of concurrent threads
        self.thread_pool = ThreadPoolExecutor(max_workers=10)

    # Initialization Methods

    def _generate_address(self, port=None):
        """
        Generate a unique address (IP, Port) for the node.
        """
        # Simulate unique IPs in a private network range (192.168.x.x)
        ip = f"192.168.{np.random.randint(0, 256)}.{np.random.randint(1, 256)}"
        port = port or self._generate_port() #np.random.randint(1024, 65535)  # Random port if not provided
        return (ip, port)

    def _generate_id(self, address):
        """
        Generate a unique node ID by hashing the address.
        """
        address_str = f"{address[0]}:{address[1]}"
        sha1_hash = hashlib.sha1(address_str.encode()).hexdigest()
        node_id = sha1_hash[-HASH_HEX_DIGITS:]  # Take the last 128 bits
        return node_id
    
    def get_excluded_ports(self):
        """
        Retrieve the list of excluded ports from Windows (netsh) or Linux (/proc/sys/net/ipv4/ip_local_reserved_ports).
        """
        excluded_ports = []

        if platform.system() == "Windows":
            try:
                # Run netsh command to get reserved ports
                output = subprocess.check_output(["netsh", "int", "ipv4", "show", "excludedportrange", "protocol=tcp"], text=True, shell=True)

                # Extract port ranges using regex
                matches = re.findall(r"(\d+)\s+(\d+)", output)
                for start, end in matches:
                    excluded_ports.append((int(start), int(end)))

            except subprocess.CalledProcessError as e:
                print(f"Failed to retrieve excluded ports on Windows: {e}")

        elif platform.system() == "Linux":
            try:
                # Use 'ss' get occupied ports
                output = subprocess.check_output(["ss", "-tan"], text=True)
                
                # Extract port numbers from the output
                matches = re.findall(r":(\d+)", output)
                occupied_ports = {int(port) for port in matches}
                
                # Convert occupied ports to (port, port) format for consistency with Windows
                for port in occupied_ports:
                    excluded_ports.append((port, port))

            except subprocess.CalledProcessError as e:
                print(f"Failed to retrieve occupied ports on Linux using 'ss': {e}")

        return excluded_ports
    
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

        self.thread_pool.submit(self._update_finger_table_scheduler)
        self.thread_pool.submit(self._update_successors_scheduler)
    
    def _update_finger_table_scheduler(self):
        interval = 5  # seconds
        while True:
            if not self.running:
                break
            time.sleep(interval)
            self.update_finger_table()
            print("Updated Finger Table of Node:", self.node_id)
    
    def _update_successors_scheduler(self):
        interval = 0.1  # seconds
        while True:
            if not self.running:
                break
            time.sleep(interval)
            self.update_successors_on_join()
            self.update_successors_on_leave()
            print(".", end="", flush=True)


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
            print(f"Node {self.node_id} listening on {self.address} (bound to {bind_address})")

            while True:
                conn, addr = s.accept()  # Accept incoming connection
                # Submit the connection to the thread pool for handling
                self.thread_pool.submit(self._handle_request, conn)

    def _handle_request(self, conn):
        try:
            data = conn.recv(1024*1024)  # Read up to 1024*1024 bytes of data
            request = pickle.loads(data)  # Deserialize the request
            operation = request["operation"]
            # print(f"Node {self.node_id}: Handling Request: {request}")
            response = None

            if operation == "FIND_SUCCESSOR":
                response = self._handle_find_successor(request)
            if operation == "DELETE_SUCCESSOR_KEYS":
                response = self._handle_delete_successor_keys(request)
            if operation == "SET_SUCCESSOR":
                response = self._handle_set_successor(request)
            if operation == "SET_PREDECESSOR":
                response = self._handle_set_predecessor(request)
            if operation == "INSERT_KEY":
                response = self._handle_insert_key_request(request)
            if operation == "DELETE_KEY":
                response = self._handle_delete_key_request(request)
            if operation == "UPDATE_KEY":
                response = self._handle_update_key_request(request)
            if operation == "LOOKUP":
                response = self._handle_lookup_request(request)

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
                response = s.recv(1024*1024)  # Receive the response
            except Exception as e:
                print(f"Error connecting to {connect_address}: {e}")
                return None

        return pickle.loads(response)  # Deserialize the response
    
    #############################
    ######### Requests ##########
    #############################
    
    def request_find_successor(self, key, node, hops):
        get_successor_request = {
            "operation": "FIND_SUCCESSOR",
            "key": key,
            "hops": hops
        }
        # Get the possition on the ring
        return self.send_request(node, get_successor_request)
    
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
        request["hops"].append(self.node_id)
        if self.node_id == key:
            return self.node_id, request["hops"]
        if distance(self.node_id, key) <= distance(self.get_successor(), key):
            return self.get_successor(), request["hops"]
        else:
            closest_preceding_node_id = self.closest_preceding_node(self, key)
            closest_preceding_node = self.network.nodes[closest_preceding_node_id]
            return self.request_find_successor(key, closest_preceding_node, request["hops"])
    
    def _handle_delete_successor_keys(self, request):
        keys = request["keys"]
        for key in keys:
            self.kd_tree.delete_points(key)
        return 0

    def _handle_set_successor(self, request):
        successor_id = request["successor"]
        self.finger_table[0] = successor_id
        self.successors[0] = successor_id
        return 0
    
    def _handle_set_predecessor(self, request):
        predecessor_id = request["predecessor"]
        self.predecessor = predecessor_id
        return 0
    
    #############################
    ####### KEY Handlers ########
    #############################

    def _handle_insert_key_request(self, request):
        """
        Handle an INSERT_KEY operation.
        """        
        key = request["key"]
        point = request["point"]
        review = request["review"]
        hops = request["hops"]
        country = request["country"]
        country_key = hash_key(country)

        del request["key"]
        del request["operation"]

        with self.lock:
            if key in self.data.keys():
                self.data[key].append(request)
            else:
                self.data[key] = []
                self.data[key].append(request)

            if self.kd_tree == None:
                # Initialize KDTree with the first point
                self.kd_tree = KDTree(
                    points=np.array([point]),
                    reviews=np.array([review]),
                    country_keys=np.array([country_key]),
                    countries=np.array([country]),
                )
            else:
                # Add point to the existing KDTree
                self.kd_tree.add_point(point, review, country)

            # # Print the point and review directly after adding
            # print(f"\nInserted Key: {key}")
            # print(f"Point: {point}")
            # print(f"Review: {review}")
            # print(f"Routed and stored at Node ID: {self.node_id}")
            # print("")
            return {
                "status": "success",
                "message": f"Key {key} stored at {self.node_id}",
                "hops": hops
            }
    
    def _handle_delete_key_request(self, request):
        """
        Handle a DELETE_KEY operation.
        """
        key = request["key"]
        hops = request["hops"]

        with self.lock:
            if key in self.data.keys():
                    del self.data[key]
                    if key in self.kd_tree.country_keys:
                        print(f"\nNode {self.node_id}: Deleted Key {key}.")
                        self.kd_tree.delete_points(key)
                    else:
                        print(f"\nNode {self.node_id}: No data for key {key}.\n")
                        return {"status": "failure", "message": f"No data for key {key} on kdtree."}
            else:
                print(f"\nNode {self.node_id}: No data for key {key}.\n")
                return {"status": "failure", "message": f"No data for key {key}."}

        return {"status": "success", "message": f"Deleted Key {key}.", "hops": hops}

    def _handle_update_key_request(self, request):
        """
        Handle an UPDATE_KEY operation with criteria and update fields.
        """
        key = request["key"]
        criteria = request.get("criteria", None)  # Optional criteria to filter
        update_fields = request["data"]  # Update fields for the KDTree
        hops = request.get("hops", [])
        
        with self.lock:
            if key in self.data.keys():
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
    
    def _handle_lookup_request(self, request): 
        """
        Handle a LOOKUP operation.
        """
        key = request["key"]
        lower_bounds = request["lower_bounds"]
        upper_bounds = request["upper_bounds"]
        N = request["N"]
        hops = request.get("hops", [])  # Retrieve the current hops list

        # with self.lock():
        if key in self.data.keys():
            print(f"\nNode {self.node_id}: Lookup Key {key} Found.")

            # If the KDTree is not initialized or has no data, return a failure message
            if not self.kd_tree or self.kd_tree.points.size == 0:
                print(f"Node {self.node_id}: No data for key {key}.")
                return {"status": "failure", "message": f"No data for key {key}."}

            # KDTree Range Search
            points, reviews = self.kd_tree.search(key, lower_bounds, upper_bounds)
            print(f"Node {self.node_id}: Found {len(points)} matching points.")

            if len(reviews) == 0:
                print(f"Node {self.node_id}: No reviews found within the specified range.")
                return {"status": "success", "points": [], "reviews": [], "hops": hops}

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
        else:
            return {"status": "failure", "message": f"Key {key} not found.", "hops": hops}
    
    #############################
    ###### KEY operations #######
    #############################

    def insert_key(self, key, point, review, country):
        """
        Initiate the INSERT_KEY operation for a given key, point, and review.
        """
        request = {
            "operation": "INSERT_KEY",
            "key": key,
            "point": point,
            "review": review,
            "country": country,
            "hops": [],  # Initialize hops tracking
        }
        successor_id, hops = self._handle_find_successor(request)
        request["hops"] = len(hops)-1
        successor = self.network.nodes[successor_id]
        
        return self.send_request(successor, request)

    def delete_key(self, key):
        """
        Delete a key from the network.
        """
        request = {
            "operation": "DELETE_KEY",
            "key": key,
            "hops": [],  # Initialize hops tracking
        }
        successor_id, hops = self._handle_find_successor(request)
        request["hops"] = len(hops)-1
        successor = self.network.nodes[successor_id]

        return self.send_request(successor, request)
    
    def update_key(self, key, updated_data, criteria=None):
        """
        Initiate the UPDATE_KEY operation for a given key with optional criteria and updated data.

        Args:
            key (str): The key (hashed country) to be updated.
            updated_data (dict): Fields to update. Example: {"attributes": {"price": 30.0}, "review": "Updated review"}.
            criteria (dict, optional): Criteria for selecting points to update.
                                    Example: {"review_date": 2019, "rating": 94}.

        Returns:
            dict: Response from the update operation, indicating success or failure.
        """
        request = {
            "operation": "UPDATE_KEY",
            "key": key,
            "data": updated_data,
            "criteria": criteria,  # Optional criteria for filtering
            "hops": [],  # Initialize hops tracking
        }
        # print(f"Node {self.node_id}: Handling Update Request: {request}")
        successor_id, hops = self._handle_find_successor(request)
        request["hops"] = len(hops)-1
        successor = self.network.nodes[successor_id]

        return self.send_request(successor, request)
  
    def lookup(self, key, lower_bounds, upper_bounds, N=5):
        """
        Lookup operation for a given key with KDTree range search and LSH similarity check.
        """
        request = {
            "operation": "LOOKUP",
            "key": key,
            "lower_bounds": lower_bounds,
            "upper_bounds": upper_bounds,
            "N": N,
            "hops": [],
        }
        successor_id, hops = self._handle_find_successor(request)
        request["hops"] = len(hops)-1
        successor = self.network.nodes[successor_id]

        return self.send_request(successor, request)

    #############################
    #### Update Finger Table ####
    #############################

    def update_finger_table(self, hops=[]):
        for i in range(1, len(self.finger_table)):
            key = int_to_hex((int(self.node_id, 16) + 2 ** i) % R)
            temp_node = self.request_find_successor(key, self, hops)[0]
            while self.network.nodes[temp_node].running == False:
                temp_node = self.request_find_successor(int_to_hex((int(temp_node, 16) + 1) % R), self, hops)[0]
            self.finger_table[i] = temp_node


    #############################
    ## Closest Preceding Node ###
    #############################

    def closest_preceding_node(self, node, h_key):
        for i in range(len(node.finger_table)-1, 0, -1):
            if distance(node.finger_table[i-1], h_key) < distance(node.finger_table[i], h_key):
                if self.network.nodes[node.finger_table[i-1]].running: # skip non-running nodes
                    return node.finger_table[i-1]

        return node.finger_table[-1]


    #############################
    ######## Node Join ##########
    #############################

    def join(self, successor_node):
        suc_id = successor_node.node_id
        pre_id = successor_node.predecessor
        
        # set the successor of the predecessor to self's id
        self.request_set_successor(self.node_id, pre_id)
        # set the predecessor of the successor to self's id
        self.request_set_predecessor(self.node_id, suc_id)
        self.finger_table[0] = suc_id
        self.successors[0] = suc_id
        self.predecessor = pre_id

        self.update_finger_table()
        
        if self.network.nodes[self.get_successor()].kd_tree == None:
            return

        # Get keys from successor
        keys = {key for key in sorted(successor_node.kd_tree.country_keys)
                if (distance(self.node_id, key) < distance(self.get_successor(), key))}
        
        # Insert keys and data to self's kdtree
        for key in keys:
            review = successor_node.kd_tree.reviews[successor_node.kd_tree.country_keys == key][0]
            country = successor_node.kd_tree.countries[successor_node.kd_tree.country_keys == key][0]
            point = successor_node.kd_tree.points[successor_node.kd_tree.country_keys == key][0]
            self.kd_tree.add_point(point, review, country)

        # Delete keys and data from successor
        self.request_delete_successor_keys(keys, suc_id)




        # # Get keys from successor
        # self.data = {key: successor_node.data[key] for key in sorted(
        #     successor_node.data.keys()) if key <= self.node_id}

        # keys_to_delete_from_successor = []
        # for key in sorted(self.data.keys()):
        #     if key in successor_node.data:
        #         keys_to_delete_from_successor.append(key)
        
        # # Delete keys
        # self.request_delete_successor_keys(keys_to_delete_from_successor, suc_id)


    #############################
    ######## NODE LEAVE #########
    #############################

    def leave(self):
        self.running = False

        # pre_id = self.predecessor
        # suc_id = self.get_successor()

        # # set the successor of the predecessor to self's successor
        # self.request_set_successor(suc_id, pre_id)
        # # set the predecessor of the successor to self's predecessor
        # self.request_set_predecessor(pre_id, suc_id)

        # # Transfer keys to successor
        # for key in sorted(self.data.keys()):
        #     self.successor.data[key] = self.data[key]


    #############################
    ###### Get Successor #######
    #############################

    def get_successor(self):
        for i in range(len(self.successors)):
            if self.network.nodes[self.successors[i]].running:
                return self.successors[i]
        return -1
    
    #############################
    #### Update Successors ######
    #############################
    
    def update_successors_on_join(self):
        # Find node to insert node
        index_to_insert_node = -1
        node_id = ""
        
        for i in range(len(self.successors)-1):
            successor_of_successor = self.network.nodes[self.successors[i]].get_successor()
            if successor_of_successor != self.successors[i+1]:
                index_to_insert_node = i + 1
                node_id = successor_of_successor
                break

        if index_to_insert_node == -1:
            return

        # Insert node
        for i in range(index_to_insert_node, len(self.successors)-1):
            self.successors[i+1] = self.successors[i]
        self.successors[index_to_insert_node] = node_id

    def update_successors_on_leave(self):
        # Find index of the node that left
        index_of_node_that_left = -1
        for i in range(len(self.successors)):
            if not self.network.nodes[self.successors[i]].running:
                index_of_node_that_left = i
                break

        if index_of_node_that_left == -1:
            return
        
        # For each index in the successors list
        for i in range(index_of_node_that_left, len(self.successors)-1):
            self.successors[i] = self.successors[i+1]
        
        # Request is not necessary
        self.successors[-1] = self.request_find_successor(int_to_hex((int(self.successors[-2], 16)+1) % R), self.successors[-2], [])[0]

        # Recursively update successors
        self.update_successors_on_leave()

