# 13 Predefined node IDs.
predefined_ids = [
    "4b12",
    "fa35",
    "19bd",
    "37de",
    "3722",
    "cafe",
    "c816",
    "fb32",
    "ca12",
    "20bc",
    "20bd",
    "3745",
    "d3ad",
]

"""------------ Chord Constants ------------"""
# Number of hex digits in a Node ID
HASH_HEX_DIGITS = 4


M = 16  # Bit size for the hash space (e.g. if M=4, 2^4 = 16 nodes)
R = 2**M  # Max possible number of nodes in the network
S = 4  # Number of successors for each node

# Operations for testing
chord_operations = ["Node Join", "Insert Keys", "Delete Keys", "Update Keys", "Lookup Keys"]


"""------------ Pastry Constants ------------"""
# Number of hex digits in a Node ID
HASH_HEX_DIGITS = 4

# Routing Base. 2^b = Number of posssible entries in each row of the routing table
b = 4

# Replication Factor
r = 2
# Number of Nodes to store in the Leaf Set
L = 2 * r

# Number of nodes to store in the neighbourhood set
NEIGHBORHOOD_SIZE = 3

# The main operations
main_operations = ["NODE_JOIN", "NODE_LEAVE", "INSERT_KEY", "LOOKUP", "UPDATE_KEY", "DELETE_KEY"]

# Operations for testing
pastry_operations = [
    "Node Join",
    "Node Leave",
    "Insert Keys",
    "Delete Keys",
    "Update Keys",
    "Lookup Keys",
]
