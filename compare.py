import json
import matplotlib.pyplot as plt
import sys
import numpy as np


def load_json_file(file_path):
    """Load and return JSON data from a file."""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: File {file_path} is not valid JSON")
        sys.exit(1)


def create_comparison_plot(chord_data, pastry_data):
    """Create a bar plot comparing Chord and Pastry metrics."""
    # Define the metrics we want to compare in order
    metrics = ["Node Join", "Insert Keys", "Update Keys", "Lookup Keys", "Delete Keys"]

    # Prepare data with consistent keys
    def process_data(data):
        values = []
        for key in data.keys():
            if key in metrics:
                values.append(data[key])
        return values

    chord_values = process_data(chord_data)
    pastry_values = process_data(pastry_data)

    # Get values in specified order
    # chord_values = [chord_vals.get(metric, 0) for metric in metrics]
    # pastry_values = [pastry_vals.get(metric, 0) for metric in metrics]

    # Set up the plot
    fig, ax = plt.subplots(figsize=(12, 6))

    # Bar settings
    width = 0.35
    x = np.arange(len(metrics))

    # Create bars
    rects1 = ax.bar(x - width / 2, chord_values, width, label="Chord")
    rects2 = ax.bar(x + width / 2, pastry_values, width, label="Pastry")

    # Customize plot
    ax.set_ylabel("Average Hops")
    ax.set_title("DHT Performance Comparison: Chord vs Pastry")
    ax.set_xticks(x)
    ax.set_xticklabels(metrics, ha="center")
    ax.legend()

    # Add value labels
    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate(
                f"{height:.2f}",
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
            )

    autolabel(rects1)
    autolabel(rects2)

    plt.tight_layout()
    plt.savefig("Comparison Plot/dht_performance_comparison.png")
    plt.close()


def main():
    # Load data from both files
    chord_data = load_json_file("Chord/ChordResults.json")
    pastry_data = load_json_file("Pastry/Tests/Hops/PastryResults.json")

    # Create the comparison plot
    create_comparison_plot(chord_data, pastry_data)
    print("Comparison plot saved as 'dht_performance_comparison.png'")


if __name__ == "__main__":
    main()
