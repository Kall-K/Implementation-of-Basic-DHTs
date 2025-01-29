# Number of hex digits in a Node ID
HASH_HEX_DIGITS = 4

# Routing Base. 2^b = Number of posssible entries in each row of the routing table
b = 4

# Total Number of Nodes in the Network
# N = 12

# Replication Factor
r = 2
# Number of Nodes to store in the Leaf Set
L = 2 * r

# Number of nodes to store in the neighbourhood set
M = 3

# The main operations
main_operations = ["NODE_JOIN", "NODE_LEAVE", "INSERT_KEY", "LOOKUP", "UPDATE_KEY", "DELETE_KEY"]
