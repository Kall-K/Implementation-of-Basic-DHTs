import json
import matplotlib.pyplot as plt
import os
import logging
import sys


def load_json_file(file_path):
    """Load and return JSON data from a file."""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Error: File {file_path} not found")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(f"Error: File {file_path} is not valid JSON")
        sys.exit(1)


def create_pastry_plot(pastry_data):
    """Create a bar plot showing average hops per operation for Pastry."""
    # Define the metrics we want to include in the plot
    metrics = pastry_data.keys()

    # Extract values for the specified metrics
    values = [pastry_data.get(metric, 0) for metric in metrics]

    # Set up the plot
    fig, ax = plt.subplots(figsize=(10, 6))

    # Create bars
    bars = ax.bar(metrics, values, color="skyblue")

    # Customize plot
    ax.set_ylabel("Average Hops")
    ax.set_title("Pastry DHT: Average Hops per Operation")
    ax.set_xticks(range(len(metrics)))
    ax.set_xticklabels(metrics, ha="center")

    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        ax.annotate(
            f"{height:.2f}",
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 3),  # 3 points vertical offset
            textcoords="offset points",
            ha="center",
            va="bottom",
        )

    # Adjust layout to prevent label cutoff
    plt.tight_layout()

    # Ensure output directory exists
    output_dir = "Plots"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Save the plot
    output_path = os.path.join(output_dir, "pastry_hops.png")
    plt.savefig(output_path)
    plt.close()
    logging.info(f"Plot saved as {output_path}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # Load data from the Pastry results file
    pastry_data = load_json_file("PastryResults.json")

    # Create the Pastry performance plot
    create_pastry_plot(pastry_data)


if __name__ == "__main__":
    main()
