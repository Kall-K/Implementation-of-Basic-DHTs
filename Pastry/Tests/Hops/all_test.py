import json
import sys
import os

from build_test import build_test
from delete_test import delete_test
from leave_test import leave_test
from lookup_test import lookup_test
from update_test import update_test

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from Pastry.network import PastryNetwork
from constants import pastry_operations


def main():
    Results = {}

    network = PastryNetwork()

    # dataset_path = "../../../Coffee_Reviews_Dataset/simplified_coffee.csv"

    # Node Join and Insert Keys Test
    input("Build the network. Press Enter to continue...")
    avg_join_hops, avg_insert_hops = build_test(network)

    Results[pastry_operations[0]] = avg_join_hops
    Results[pastry_operations[2]] = avg_insert_hops

    # Lookup Test
    input("Test Lookup. Press Enter to continue...")
    avg_lookup_hops = lookup_test(network)
    Results[pastry_operations[5]] = avg_lookup_hops

    # Update Test
    input("Test Update. Press Enter to continue...")
    avg_update_hops = update_test(network)
    Results[pastry_operations[4]] = avg_update_hops

    # Delete Test
    input("Test Delete. Press Enter to continue...")
    avg_delete_hops = delete_test(network)
    Results[pastry_operations[3]] = avg_delete_hops

    # Leave Test
    input("Test Leave. Press Enter to continue...")
    avg_leave_hops = leave_test(network)
    Results[pastry_operations[1]] = avg_leave_hops

    with open("PastryResults.json", "a") as outfile:
        json.dump(Results, outfile, indent=4)

    # Show the DHT GUI
    # network.gui.show_dht_gui()
    # network.gui.root.mainloop()


if __name__ == "__main__":
    main()
