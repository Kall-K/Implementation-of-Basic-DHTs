import pandas as pd
import numpy as np
from sklearn.neighbors import KDTree
from datetime import datetime

# Step 1: Load CSV file
csv_file = "Coffee Reviews Dataset/simplified_coffee.csv"  # Replace with your file path
df = pd.read_csv(csv_file)

# Step 2: Preprocess the data
# Convert review_date to ordinal (numeric representation of date)
df["review_date"] = pd.to_datetime(df["review_date"], format="%B %Y").map(
    datetime.toordinal
)

# Extract the 3D data: review_date, rating, 100g_USD
points = df[["review_date", "rating", "100g_USD"]].to_numpy()

# Step 3: Build the KD-Tree
kd_tree = KDTree(points)


# Step 4: Define methods
def add_node(kd_tree, new_point):
    """
    Add a new point to the KD-Tree by rebuilding it with the new point added.
    """
    global points
    points = np.vstack([points, new_point])
    return KDTree(points)


def range_search(kd_tree, min_range, max_range):
    """
    Perform a range search on the KD-Tree.
    """
    min_range = np.array(min_range)
    max_range = np.array(max_range)
    indices = kd_tree.query_radius([min_range], r=0)
    results = points[indices[0]]
    results = results[
        (results[:, 0] >= min_range[0])
        & (results[:, 0] <= max_range[0])
        & (results[:, 1] >= min_range[1])
        & (results[:, 1] <= max_range[1])
        & (results[:, 2] >= min_range[2])
        & (results[:, 2] <= max_range[2])
    ]
    return results


# Step 5: Example usage
# Add a new point
new_point = [datetime(2018, 1, 1).toordinal(), 95, 4.50]  # Example new node
kd_tree = add_node(kd_tree, new_point)

# Range search
min_range = [
    datetime(2017, 11, 1).toordinal(),
    91,
    3.5,
]  # Min values for (review_date, rating, 100g_USD)
max_range = [
    datetime(2017, 12, 31).toordinal(),
    94,
    5.5,
]  # Max values for (review_date, rating, 100g_USD)
results = range_search(kd_tree, min_range, max_range)

print("Range Search Results:")
print(results)
