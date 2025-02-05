import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from Pastry.network import PastryNetwork

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


def main():
    network = PastryNetwork()

    # network.build(node_num=20, dataset_path="../../Coffee_Reviews_Dataset/simplified_coffee.csv")
    avg_join_hops, avg_insert_hops = network.build(
        predefined_ids=predefined_ids,
        dataset_path="../../Coffee_Reviews_Dataset/simplified_coffee.csv",
    )

    print(f"\nAverage Hops during Node Arrivals: {avg_join_hops}")
    print(f"\nAverage Hops during key Insertion: {avg_insert_hops}\n")


if __name__ == "__main__":
    main()
