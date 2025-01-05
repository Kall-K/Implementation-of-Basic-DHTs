import threading
import socket
import pickle
import numpy as np

from Multidimensional_Data_Structures.kd_tree import KDTree
from Multidimensional_Data_Structures.lsh import LSH
from sklearn.feature_extraction.text import TfidfVectorizer


class PastryNode:
    HEX_DIGITS = 16
    ID_BITS = 128

    def __init__(self, node_id, address, network):
        # Validate the node ID
        if not isinstance(node_id, str) or len(node_id) != PastryNode.ID_BITS // 4:
            raise ValueError("Node ID must be a 128-bit hexadecimal string.")
        self.node_id = node_id
        self.address = address  # (IP, Port), required to be used with sockets
        self.network = network  # Reference to the DHT network
        self.kd_tree = None  # Centralized KD-Tree
        # Routing table: 2D array with rows=prefix length, cols=16 for hex digits
        self.routing_table = np.full(
            (PastryNode.ID_BITS // 4, PastryNode.HEX_DIGITS), None, dtype=object
        )
        self.leaf_set = []  # Closest nodes numerically
        self.neighborhood_set = []  # Nearby nodes based on latency
        self.lock = threading.Lock()

    def start_server(self):
        server_thread = threading.Thread(target=self._server)
        server_thread.start()

    # Sets up a server for the node to listen for incoming requests from other nodes
    def _server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(self.address)
            s.listen()
            print(f"Node {self.node_id} listening on {self.address}")
            while True:
                # conn is a new socket object usable to send and receive data on the connection
                # addr is the address of the client (IP, Port)
                conn, addr = s.accept()
                # Spawn a new thread to handle the request
                threading.Thread(target=self._handle_request, args=(conn,)).start()

    def update_routing_table(self, new_node_id):
        """
        Update the routing table with a new node's information.
        """
        common_prefix_length = self._common_prefix_length(self.node_id, new_node_id)
        next_digit = int(new_node_id[common_prefix_length], 16)  # Convert hex to int
        self.routing_table[common_prefix_length, next_digit] = new_node_id

    def _common_prefix_length(self, id1, id2):
        """
        Compute the length of the common prefix between two node IDs.
        """
        for i in range(len(id1)):
            if id1[i] != id2[i]:
                return i
        return len(id1)

    def _handle_request(self, conn):
        data = conn.recv(1024)  # Reads up to 1024 bytes of data from the connection
        request = pickle.loads(data)
        operation = request["operation"]
        response = None

        if operation == "insert":
            response = self._insert_key(request["key"], request["value"])
        elif operation == "delete":
            response = self._delete_key(request["key"])
        elif operation == "lookup":
            response = self._lookup_key(request["key"])
        elif operation == "range_search":
            response = self.kd_tree.search(
                request["lower_bounds"], request["upper_bounds"]
            )

        conn.sendall(
            pickle.dumps(response)
        )  # Serializes the response and sends it back to the client
        conn.close()

    def _insert_key(self, key, value):
        # Route key to the responsible node
        if self._is_responsible(key):
            point, review = key, value
            self.kd_tree.add_point(point, review)
            return "Key inserted locally"
        else:
            next_hop = self._find_next_hop(key)
            self._send_request(
                next_hop, {"operation": "insert", "key": key, "value": (point, review)}
            )

    """def _delete_key(self, key):
        if self._is_responsible(key):
            # Implement key deletion logic in KD-Tree
            pass
        else:
            next_hop = self._find_next_hop(key)
            self._send_request(next_hop, {"operation": "delete", "key": key})"""

    """def _lookup_key(self, key):
        if self._is_responsible(key):
            # Return the value stored in KD-Tree
            pass
        else:
            next_hop = self._find_next_hop(key)
            return self._send_request(next_hop, {"operation": "lookup", "key": key})"""

    """def _similarity_search(self, reviews):
        # Use LSH to find similar reviews
        vectorizer = TfidfVectorizer()
        doc_vectors = vectorizer.fit_transform(reviews).toarray()
        lsh = LSH(num_bands=4, num_rows=5)
        for vector in doc_vectors:
            lsh.add_document(vector)
        similar_pairs = lsh.find_similar_pairs()
        return similar_pairs"""

    def _is_responsible(self, key):
        # Check if this node is responsible for the given key
        return True

    def _find_next_hop(self, key):
        # Use the routing table to find the next node
        pass

    def _send_request(self, node_address, request):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(node_address)
            s.sendall(pickle.dumps(request))
            response = s.recv(1024)
        return pickle.loads(response)
