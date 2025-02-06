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
from collections import defaultdict
import sys
import os

# Add the parent directory to sys.path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from .constants import *
from .helper_functions import *
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
        self.node_id = node_id if node_id is not None else self._generate_id(self.address)
        self.kd_tree = None  # Centralized KD-Tree

        self.predecessor = self.node_id
        self.finger_table = [self.node_id] * M

        self.successors = [self.node_id] * S
        self.running = True

        self.back_up = None  # For storing key-value pairs

        self.lock = threading.Lock()  # Lock for thread safety

        # Create a thread pool for handling requests to limit the number of concurrent threads
        self.thread_pool = ThreadPoolExecutor(max_workers=10)
        self.stop_event = threading.Event()  # event to stop while

    # Initialization Methods

    def _generate_address(self, port=None):
        """
        Generate a unique address (IP, Port) for the node.
        """
        # Simulate unique IPs in a private network range (192.168.x.x)
        ip = f"192.168.{np.random.randint(0, 256)}.{np.random.randint(1, 256)}"
        port = (
            port or self._generate_port()
        )  # np.random.randint(1024, 65535)  # Random port if not provided
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
                output = subprocess.check_output(
                    ["netsh", "int", "ipv4", "show", "excludedportrange", "protocol=tcp"],
                    text=True,
                    shell=True,
                )

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
    def get_state(self):
        """
        Return a string containing the state of the node (ID, Address, Data Structures).
        """
        state = []
        state.append(f"Node ID: {self.node_id}")
        state.append(f"Port: {self.address[1]}")
        state.append(f"\nPredecessor: {self.predecessor}")
        state.append(f"Successors: {self.successors}")
        state.append(f"\nFinger Table: {self.finger_table}")

        # KD Tree Information
        state.append("\nKD Tree:\nUnique Country Keys:")
        if not self.kd_tree or self.kd_tree.country_keys.size == 0:
            state.append("[]")  # Empty KD Tree
        else:
            unique_keys, counts = np.unique(self.kd_tree.country_keys, return_counts=True)
            state.append(f"{list(map(str, unique_keys))}")

            # Country count table
            state.append("\nNumber of points/reviews for each country:")
            state.append(f"{'Country Key':<12} | {'Country Name':<14} | {'Count':<6}")
            state.append("-" * 38)

            country_map = dict(zip(self.kd_tree.country_keys, self.kd_tree.countries))
            for key, count in zip(unique_keys, counts):
                state.append(f"{key:<12} | {country_map[key]:<14} | {count:<6}")

        state.append("\nBackup:\nUnique Country Keys:")
        if not self.back_up or self.back_up.country_keys.size == 0:
            state.append("[]")  # Empty Backup
        else:
            unique_keys, counts = np.unique(self.back_up.country_keys, return_counts=True)
            state.append(f"{list(map(str, unique_keys))}")

            # Country count table
            state.append("\nNumber of points/reviews for each country:")
            state.append(f"{'Country Key':<12} | {'Country Name':<14} | {'Count':<6}")
            state.append("-" * 38)

            country_map = dict(zip(self.back_up.country_keys, self.back_up.countries))
            for key, count in zip(unique_keys, counts):
                state.append(f"{key:<12} | {country_map[key]:<14} | {count:<6}")

        return "\n".join(state)

    def print_state(self):
        print(self.get_state)


    # Network Communication

    def start_server(self):
        """
        Start the server thread to listen for incoming requests.
        """
        self.thread_pool.submit(self._server)
        self.thread_pool.submit(self._update_successors_scheduler)
        self.thread_pool.submit(self._update_finger_table_scheduler)

    def _update_finger_table_scheduler(self):
        interval = 1.5  # seconds
        while True:
            if not self.running:
                break
            time.sleep(interval)
            self.update_finger_table()
            # print("Updated Finger Table of Node:", self.node_id)

    def _update_successors_scheduler(self):
        interval = 0.5  # seconds
        while True:
            if not self.running:
                break
            time.sleep(interval)
            self.update_successors_on_join()
            self.update_successors_on_leave()
            # print(".", end=" ", flush=True)

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
            # print(f"Node {self.node_id} listening on {self.address} (bound to {bind_address})")

            while not self.stop_event.is_set():
                conn, addr = s.accept()  # Accept incoming connection
                # Submit the connection to the thread pool for handling
                try:
                    self.thread_pool.submit(self._handle_request, conn)
                except RuntimeError as e:
                    return None

    def _handle_request(self, conn):
        try:
            data = conn.recv(1024 * 1024)  # Read up to 1024*1024 bytes of data
            request = pickle.loads(data)  # Deserialize the request
            operation = request["operation"]
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
            if operation == "RESTORATION":
                response = self._handle_restoration_request(request)
            if operation == "SET_BACKUP":
                response = self._handle_set_backup(request)
            if operation == "GET_SUCCESSOR":
                response = self._handle_get_successor_request()
            if operation == "GET_STATUS":
                response = self._handle_get_status_request()

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
                response = s.recv(1024 * 1024)  # Receive the response
            except Exception as e:
                return None

        return pickle.loads(response)  # Deserialize the response

    #############################
    ######### Requests ##########
    #############################

    def request_find_successor(self, key, node, hops):
        get_successor_request = {"operation": "FIND_SUCCESSOR", "key": key, "hops": hops}
        # Get the position on the ring
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

    def request_restoration(self, successor_id):
        node = self.network.nodes[successor_id]
        restoration = {
            "operation": "RESTORATION",
            "sender_id": self.node_id,
            "kdtree": self.kd_tree,
        }
        status = self.send_request(node, restoration)
        return status

    def request_get_successor(self, node_id):
        node = self.network.nodes[node_id]
        get_successor = {"operation": "GET_SUCCESSOR"}
        status = self.send_request(node, get_successor)
        return status

    def request_status_running(self, node_id):
        node = self.network.nodes[node_id]
        get_status = {"operation": "GET_STATUS"}
        status = self.send_request(node, get_status)
        return status

    def request_set_backup(self, backup, node_id):
        node = self.network.nodes[node_id]
        set_backup = {"operation": "SET_BACKUP", "backup": backup}
        status = self.send_request(node, set_backup)
        return status

    def request_backup_update(self, node_id, request):
        node = self.network.nodes[node_id]
        request["choice"] = False
        status = self.send_request(node, request)
        return status

    #############################
    ######### Handlers ##########
    #############################

    def _handle_find_successor(self, request):
        # print(f"\n- {self.node_id} ENTERING FIND_SUCCESSOR", end = ", ")
        key = request["key"]
        request["hops"].append(self.node_id)
        if self.node_id == key:
            # print(f"1rst if", end=", ")
            return self.node_id, request["hops"]
        if distance(self.node_id, key) <= distance(self.get_successor(), key):
            # print(f"2nd if", end=", ")
            return self.get_successor(), request["hops"]
        else:
            # print(f"else1", end=", ")
            closest_preceding_node_id = self.closest_preceding_node(self, key)
            closest_preceding_node = self.network.nodes[closest_preceding_node_id]
            # print(f"else2")
            return self.request_find_successor(key, closest_preceding_node, request["hops"])

    def _handle_delete_successor_keys(self, request):
        keys = request["keys"]
        for key in keys:
            self.kd_tree.delete_points(key)
        self.request_set_backup(self.kd_tree, self.get_successor())
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

    def _handle_restoration_request(self, request):
        tree = request["kdtree"]
        # Set new predecessor
        self.predecessor = request["sender_id"]
        if self.back_up:
            # Merge back up kdtree
            keys = self.back_up.country_keys.tolist()
            points = self.back_up.points
            reviews = self.back_up.reviews.tolist()
            countries = self.back_up.countries


            for key, point, review, country in zip(keys, points, reviews, countries):

                request = {
                    "operation": "INSERT_KEY",
                    "key": key,
                    "point": point,
                    "review": review,
                    "country": country,
                    "hops": [],  # Initialize hops tracking
                    "choice": True,
                }
                self._handle_insert_key_request(request)
            self.back_up = tree
            return {"message": "Back up merged."}

        else:
            self.back_up = tree
            return {"message": "Back up is empty."}

    def _handle_set_backup(self, request):
        self.back_up = request["backup"]
        return 0

    def _handle_get_successor_request(self):
        return self.get_successor()

    def _handle_get_status_request(self):
        return self.running

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

        tree = self.kd_tree if request["choice"] else self.back_up

        with self.lock:
            if tree == None:
                # Initialize KDTree with the first point
                tree = KDTree(
                    points=np.array([point]),
                    reviews=np.array([review]),
                    country_keys=np.array([country_key]),
                    countries=np.array([country]),
                )
            else:
                # Add point to the existing KDTree
                tree.add_point(point, review, country)

            if request["choice"]:
                self.kd_tree = tree
                self.request_backup_update(self.get_successor(), request)
            else:
                self.back_up = tree

            return {
                "status": "success",
                "message": f"Key {key} stored at {self.node_id}",
                "hops": hops,
            }

    def _handle_delete_key_request(self, request):
        """
        Handle a DELETE_KEY operation.
        """
        key = request["key"]
        hops = request["hops"]

        tree = self.kd_tree if request["choice"] else self.back_up

        with self.lock:
            if tree and key in tree.country_keys:
                tree.delete_points(key)
            else:
                return {"status": "failure", "message": f"No data for key {key}.", "hops": hops}
            
            if request["choice"]: 
                self.kd_tree = tree
                self.request_backup_update(self.get_successor(), request)
            else: 
                self.back_up = tree

            return {"status": "success", "message": f"Deleted Key {key}.", "hops": hops}

    def _handle_update_key_request(self, request):
        """
        Handle an UPDATE_KEY operation with criteria and update fields.
        """
        key = request["key"]
        criteria = request.get("criteria", None)  # Optional criteria to filter
        update_fields = request["data"]  # Update fields for the KDTree
        hops = request.get("hops", [])

        tree = self.kd_tree if request["choice"] else self.back_up

        with self.lock:
            if self.kd_tree and key in self.kd_tree.country_keys:
                # Update the data in the KDTree
                self.kd_tree.update_points(
                    country_key=key,
                    criteria=criteria,
                    update_fields=update_fields,
                )
            else:
                return {"status": "failure", "message": f"Key {key} not found.", "hops": hops}

            if request["choice"]:
                self.kd_tree = tree
                self.request_backup_update(self.get_successor(), request)
            else:
                self.back_up = tree

            return {
                "status": "success",
                "message": f"Key {key} updated successfully.",
                "hops": hops,
            }

    def _handle_lookup_request(self, request):
        """
        Handle a LOOKUP operation.
        """
        key = request["key"]
        lower_bounds = request["lower_bounds"]
        upper_bounds = request["upper_bounds"]
        N = request["N"]
        hops = request.get("hops", [])  # Retrieve the current hops list

        if (
            key not in self.kd_tree.country_keys
            or not self.kd_tree
            or self.kd_tree.points.size == 0
        ):
            print(f"Node {self.node_id}: No data for key {key}.")

            return {"status": "failure", "message": f"No data for key {key}.", "hops": hops}
        
        # KDTree Range Search
        points, reviews = self.kd_tree.search(key, lower_bounds, upper_bounds)
        # print(f"Node {self.node_id}: Found {len(points)} matching points.")

        if len(reviews) == 0:
            print(f"Node {self.node_id}: No reviews found within the specified range.")
            return {
                "status": "success",
                "points": [],
                "reviews": [],
                "similar_reviews": [],
                "hops": hops,
            }

        # LSH Similarity Search
        vectorizer = TfidfVectorizer()
        doc_vectors = vectorizer.fit_transform(reviews).toarray()

        lsh = LSH(num_bands=4, num_rows=5)
        for vector in doc_vectors:
            lsh.add_document(vector)

        similar_pairs = lsh.find_similar_pairs(N)
        similar_docs = lsh.find_similar_docs(similar_pairs, reviews, N)

        if similar_docs:
            print(f"\nThe {N} Most Similar Reviews:\n")
            for i, doc in enumerate(similar_docs, 1):
                print(f"{i}. {doc}\n")

        return {
            "status": "success",
            "points": points,
            "reviews": reviews,
            "similar_reviews": similar_docs,
            "message": f"Node {self.node_id} found {len(points)} matching points.",
            "hops": hops,
        }

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
        request["hops"] = len(hops) - 1
        successor = self.network.nodes[successor_id]
        request["choice"] = True
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
        request["hops"] = len(hops) - 1
        successor = self.network.nodes[successor_id]
        request["choice"] = True
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
        successor_id, hops = self._handle_find_successor(request)
        request["hops"] = len(hops) - 1
        successor = self.network.nodes[successor_id]
        request["choice"] = True
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
        request["hops"] = len(hops) - 1
        successor = self.network.nodes[successor_id]

        return self.send_request(successor, request)

    #############################
    #### Update Finger Table ####
    #############################
    
    def update_finger_table(self, hops=[]):
        # print(f"\n- {self.node_id} ENTERING Update Finger Table -", end=", ")
        self.finger_table[0] = self.get_successor()
        for i in range(1, len(self.finger_table)):
            # print(f"{self.node_id} 1", end=", ")
            key = int_to_hex((int(self.node_id, 16) + 2 ** i) % R)
            temp_node = self.request_find_successor(key, self, hops)[0]
            # print(f"{self.node_id} 2")
            for _ in range(len(self.network.nodes)):
                if self.network.nodes[temp_node].running == True: break
                temp_node = self.request_find_successor(int_to_hex((int(temp_node, 16)+1) % R), self, hops)[0]

            self.finger_table[i] = temp_node
        # print(f"- {self.node_id} LEAVING -\n")
        
    #############################
    ## Closest Preceding Node ###
    #############################

    def closest_preceding_node(self, node, h_key):
        for i in range(len(node.finger_table)-1, 0, -1):
            preceding_node = node.finger_table[i-1]
            next_node = node.finger_table[i]

            running_node_found = False
            for _ in range(i, len(self.finger_table)):
                if self.network.nodes[node.finger_table[i]].running == True:
                    next_node = node.finger_table[i]
                    running_node_found = True
                    break
            if not running_node_found:
                next_node = node.finger_table[-1]
                
            if distance(node.finger_table[i-1], h_key) < distance(next_node, h_key):
                preceding_node = node.finger_table[i-1]
                if self.request_status_running(preceding_node):
                # if self.network.nodes[preceding_node].running: # skip non-running nodes
                    return preceding_node #  -3 |-2| key -1


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

        if successor_node.kd_tree != None:
            # Get keys from successor
            keys = [key for key in np.unique(successor_node.kd_tree.country_keys)
                    if (distance(self.node_id, key) > distance(self.get_successor(), key))]
            
            # 1. Insert keys and data to self's kdtree
            for key in keys:
                indices = np.where(successor_node.kd_tree.country_keys == key)
                reviews = successor_node.kd_tree.reviews[indices]
                countries = [successor_node.kd_tree.countries[i] for i in indices[0]]
                points = successor_node.kd_tree.points[indices]
                for review,point,country in zip(reviews, points, countries):
                    request = {
                        "operation": "INSERT_KEY",
                        "key": key,
                        "point": point,
                        "review": review,
                        "country": country,
                        "hops": [],  # Initialize hops tracking
                        "choice": True
                    }
                    self._handle_insert_key_request(request)

            # 2. Delete keys and data from successor
            self.request_delete_successor_keys(keys, suc_id)

            # 3. Update successor's backup with self's kd_tree
            self.request_set_backup(self.kd_tree, suc_id)

        # 4. Update backup from predecessor's kd_tree
        if (not self.network.nodes[pre_id].running) or (self.network.nodes[pre_id] == self.node_id):
            return
        predecessor_node = self.network.nodes[pre_id]
        self.back_up = predecessor_node.kd_tree

    #############################
    ######## NODE LEAVE #########
    #############################

    def leave(self):
        self.running = False
        self.stop_event.set()
        self.thread_pool.shutdown(wait=False)

    #############################
    ####### Get Successor #######
    #############################

    def get_successor(self):
        # Get first running successor
        for i in range(len(self.successors) - 1):
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

        for i in range(len(self.successors) - 1):
            successor_of_successor = self.network.nodes[self.successors[i]].get_successor()
            if successor_of_successor != self.successors[i + 1]:
                index_to_insert_node = i + 1
                node_id = successor_of_successor
                break

        if index_to_insert_node == -1:
            return

        # Insert node
        for i in range(index_to_insert_node, len(self.successors) - 1):
            self.successors[i + 1] = self.successors[i]
        self.successors[index_to_insert_node] = node_id
        self.update_successors_on_join()

    def update_successors_on_leave(self):
        # Find index of the node that left
        index_of_node_that_left = -1
        for i in range(len(self.successors)):
            if not self.network.nodes[self.successors[i]].running:
                index_of_node_that_left = i
                break

        if index_of_node_that_left == -1:
            return

        if index_of_node_that_left == 0:
            new_successor = self.get_successor()
            self.request_restoration(new_successor)

        # For each index in the successors list
        for i in range(index_of_node_that_left, len(self.successors) - 1):
            self.successors[i] = self.successors[i + 1]

        # update last position because its empty
        self.successors[-1] = self.request_get_successor(self.successors[-2])
        # Recursively update successors
        self.update_successors_on_leave()