class PastryNetwork:
    def __init__(self):
        self.nodes = {}  # Dictionary. Keys are node IDs, values are Node objects

    def node_join(self, new_node):
        """
        Handle a new node joining the network.
        """
        if not self.nodes:
            # First node in the network, initialize with its own state
            self.nodes[new_node.node_id] = new_node
            print(f"Node {new_node.node_id} is the first node in the network.")
            return

        # Step 1: Find the geographically closest node A
        closest_node = self._find_geographically_closest_node(new_node)

        # Step 2: Find numerically closest node Z using key lookup on A
        numerically_closest_node = closest_node.find_closest_node(new_node.node_id)

        # Step 3: Initialize the new node's tables
        self._initialize_new_node_tables(
            new_node, closest_node, numerically_closest_node
        )

        # Step 4: Add the new node to the network and broadcast state
        self.nodes[new_node.node_id] = new_node
        self._broadcast_new_node_state(new_node)
        print(f"Node {new_node.node_id} has joined the network.")
