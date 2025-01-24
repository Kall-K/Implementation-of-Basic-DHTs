import threading
import socket
import hashlib
import pickle
from concurrent.futures import ThreadPoolExecutor
import numpy as np

from constants import *
from helper_functions import *

# from Multidimensional_Data_Structures.kd_tree import KDTree
# from Multidimensional_Data_Structures.lsh import LSH
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
        
        self.successor = None
        self.predecessor = None
        self.finger_table = [None] * M
        self.data_store = {}  # For storing key-value pairs
        self.running = True

        self.lock = threading.Lock()

        # Create a thread pool for handling requests to limit the number of concurrent threads
        self.thread_pool = ThreadPoolExecutor(max_workers=10)

    # Initialization Methods

    def _generate_address(self, port=None):
        """
        Generate a unique address (IP, Port) for the node.
        """
        # Simulate unique IPs in a private network range (192.168.x.x)
        ip = f"192.168.{np.random.randint(0, 256)}.{np.random.randint(1, 256)}"
        port = port or np.random.randint(1024, 65535)  # Random port if not provided
        return (ip, port)

    def _generate_id(self, address):
        """
        Generate a unique node ID by hashing the address.
        """
        address_str = f"{address[0]}:{address[1]}"
        sha1_hash = hashlib.sha1(address_str.encode()).hexdigest()
        node_id = sha1_hash[-HASH_HEX_DIGITS:]  # Take the last 128 bits
        return node_id
    
    # Find successor

    # def find_successor(self, key):
    #     """Find the successor node for a given key."""
    #     if self.successor and (self.node_id < key <= self.successor.node_id or 
    #                            (self.successor.node_id < self.node_id <= key) or 
    #                            (key <= self.successor.node_id < self.node_id)):
    #         return self.successor
    #     else:
    #         return self.get_closest_preceding_node(key).find_successor(key)

    # Closest preceding node

    def get_closest_preceding_node(self, key):
        """Find the closest preceding node for a given key."""
        for i in range(M - 1, -1, -1):
            if self.finger_table[i] and self.node_id < self.finger_table[i].node_id < key:
                return self.finger_table[i]
        return self
    

    # Βάζει τον κόμβο στο δίκτυο
    def join(self, successor):
        suc = successor
        pre = suc.predecessor
        
        self.find_node_place(pre, suc)
        # self.update_fingers_table()

        # # Παίρνει τα keys από το successor
        # self.data = {key: self.successor.data[key] for key in sorted(
        #     self.successor.data.keys()) if key <= self.node_id}

        # for key in sorted(self.data.keys()):
        #     if key in self.successor.data:
        #         del self.successor.data[key]

# Βρίσκει τη θέση του κόμβου
    def find_node_place(self, pre, suc):
        pre.fingers_table[0] = self
        pre.successor = self
        suc.predecessor = self
        self.fingers_table[0] = suc
        self.successor = suc
        self.predecessor = pre

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
        print("\nRouting Table:")
        for row in self.routing_table:
            print(row)
        print("\nLeaf Set:")
        print(f"Lmin: {self.Lmin}")
        print(f"Lmax: {self.Lmax}")
        print("\nNeighborhood Set:")
        print(self.neighborhood_set)

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
            except Exception as e:
                print(f"Error connecting to {connect_address}: {e}")
                return None

        return pickle.loads(response)  # Deserialize the response

    def _handle_find_successor(self, request):
        node = request["node"]

        key = node.node_id
        """Find the successor node for a given key."""
        if self.successor and (self.node_id < key <= self.successor.node_id or 
                               (self.successor.node_id < self.node_id <= key) or 
                               (key <= self.successor.node_id < self.node_id)):
            return self.successor
        else:
            closest_preceding_node = self.get_closest_preceding_node(key)
            successor_request = {
                "operation": "FIND_SUCCESSOR",
                "node": node,
            }
            return self.send_request(closest_preceding_node, successor_request)


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


   