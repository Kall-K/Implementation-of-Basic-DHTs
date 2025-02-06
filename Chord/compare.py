import json
import matplotlib.pyplot as plt
import sys
import numpy as np

def load_json_file(file_path):
    """Load and return JSON data from a file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: File {file_path} is not valid JSON")
        sys.exit(1)

def create_comparison_plot(data1, data2, file1_name, file2_name):
    """Create a bar plot comparing metrics from two JSON files."""
    # Get all unique keys
    all_keys = sorted(set(data1.keys()) | set(data2.keys()))
    
    # Convert string values to numeric if possible
    for data in [data1, data2]:
        for key in data:
            if isinstance(data[key], str):
                try:
                    data[key] = float(data[key])
                except ValueError:
                    data[key] = 0  # Set non-numeric values to 0
    
    # Prepare data for plotting
    values1 = [data1.get(key, 0) for key in all_keys]
    values2 = [data2.get(key, 0) for key in all_keys]
    
    # Set up the plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Set the width of each bar and positions of the bars
    width = 0.35
    x = np.arange(len(all_keys))
    
    # Create bars
    rects1 = ax.bar(x - width/2, values1, width, label=file1_name)
    rects2 = ax.bar(x + width/2, values2, width, label=file2_name)
    
    # Customize the plot
    ax.set_ylabel('Values')
    ax.set_title('Comparison of Metrics')
    ax.set_xticks(x)
    ax.set_xticklabels(all_keys, rotation=45, ha='right')
    ax.legend()
    
    # Add value labels on top of bars
    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate(f'{height:.2f}',
                       xy=(rect.get_x() + rect.get_width() / 2, height),
                       xytext=(0, 3),  # 3 points vertical offset
                       textcoords="offset points",
                       ha='center', va='bottom')
    
    autolabel(rects1)
    autolabel(rects2)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('comparison_plot.png')
    plt.close()

def main():
    if len(sys.argv) != 3:
        print("Usage: python script.py file1.json file2.json")
        sys.exit(1)
    
    file1_path = sys.argv[1]
    file2_path = sys.argv[2]
    
    # Load data from both files
    data1 = load_json_file(file1_path)
    data2 = load_json_file(file2_path)
    
    # Create the comparison plot
    create_comparison_plot(data1, data2, 
                         file1_name=file1_path.split('/')[-1],
                         file2_name=file2_path.split('/')[-1])
    
    print("Plot has been saved as 'comparison_plot.png'")


main()