from ipaddress import ip_address
import hashlib

from constants import *

"""---Helper function for the Pasrty Implementation---"""


def hash_key(value):
    """
    Hash the input value and return the last 4 hex digits.
    """
    sha1_hash = hashlib.sha1(value.encode()).hexdigest()
    return sha1_hash[-4:]

def topological_distance(ip1, ip2):
    """
    Calculate the topological distance between two nodes based on their ip addresses.
    """
    # Convert IP addresses to their integer representation
    ip1_numeric = int(ip_address(ip1))
    ip2_numeric = int(ip_address(ip2))
    # Calculate the absolute distance
    return abs(ip1_numeric - ip2_numeric)


def common_prefix_length(id1, id2):
    """
    Compute the length of the common prefix between two node IDs.
    """
    for i in range(len(id1)):
        if id1[i] != id2[i]:
            return i
    return len(id1)


def hex_distance(id1, id2):
    """
    Calculate the distance between two hexadecimal IDs.
    """
    # Iterate through each character in the IDs
    for i in range(len(id1)):
        # Check if the characters at the current position are different
        if id1[i] != id2[i]:
            # Return the index of the first differing character and the absolute difference between the two IDs
            return i, abs(int(id1[i:], 16) - int(id2[i:], 16))
    # If the IDs are identical, return the length of the IDs and 0
    return -1, 0


"""def hex_distance(id1, id2):
    #Calculate the absolute distance between two hexadecimal IDs.
    return abs(int(id1, 16) - int(id2, 16))"""


def hex_compare(id1, id2, equality=True):
    """
    Check if id1 >= id2 if equality=True
    or id1 > id2 if equality=False
    """
    if id1 == id2:
        if equality:
            return True
        return False

    int1 = int(id1, 16)
    int2 = int(id2, 16)
    dist = int1 - int2
    if dist > 0:
        return True
    else:
        return False