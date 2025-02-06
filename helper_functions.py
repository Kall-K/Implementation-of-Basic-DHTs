import hashlib
import matplotlib.pyplot as plt

from constants import R

"""---Helper function for the Pasrty Implementation---"""


def hash_key(value):
    """
    Hash the input value and return the last 4 hex digits.
    """
    sha1_hash = hashlib.sha1(value.encode()).hexdigest()
    return sha1_hash[-4:]


def topological_distance(pos1, pos2):
    """
    Calculate the topological distance between two nodes based on their position (float between 0 and 1).
    """
    return abs(pos1 - pos2)


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
    # If the IDs are identical, the different digit is set to len(id1) and the distance is 0
    return len(id1), 0


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


def distance(hex1, hex2):
    [n1, n2] = [int(str(hex1), 16), int(str(hex2), 16)]
    if n1 <= n2:
        return n2 - n1
    else:
        return R - n1 + n2


def int_to_hex(num):
    return hex(num)[2:].rjust(4, "0")


def plot_hops(hops_counts):
    """
    Plot a bar graph for hops in different operations.
    """
    operations = list(hops_counts.keys())
    # Extract the first value (or 0 if empty) for each operation
    hops = [counts[0] if counts else 0 for counts in hops_counts.values()]

    plt.figure(figsize=(10, 6))
    plt.bar(operations, hops, color="skyblue")
    plt.title("Number of Hops for Pastry Network Operations", fontsize=16)
    plt.ylabel("Number of Hops", fontsize=14)
    plt.xlabel("Operation Type", fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.show()
