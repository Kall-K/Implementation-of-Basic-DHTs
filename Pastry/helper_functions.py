from ipaddress import ip_address

from constants import *

"""---Helper function for the Pasrty Implementation---"""


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
    Compute the absolute distance between two hexadecimal number strings.
    """
    # Convert the hexadecimal strings to integers
    int1 = int(id1, 16)
    int2 = int(id2, 16)

    # Calculate the absolute distance
    distance = abs(int1 - int2)

    return distance


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
