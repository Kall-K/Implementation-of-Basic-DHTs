# Number of hex digits in a Node ID
HASH_HEX_DIGITS = 4

# Total Number of Nodes in the Network
N = 6


M = 16   # Bit size for the hash space (e.g. if M=4, 2^4 = 16 nodes)
R = 2**M   # Max possible number of nodes in the network
S = 4   # Number of successors for each node

# Predefined node IDs to test
predefined_ids = ["4b12", "fa35", "19bd", "4bde", "4c12", "cafe"]
predefined_ids = [
    "4b12",
    "fa35",
    "19bd",
    "37de",
    "3722",
    "ca12",
    "cafe",
    "fb32",
    "20bc",
    "20bd",
    "3745",
    "d3ad",
]

# operations for testing
operations = [
    "Node Join",
    "Insert Keys",
    "Delete Keys",
    "Update Keys",
    "Look up Keys"
]