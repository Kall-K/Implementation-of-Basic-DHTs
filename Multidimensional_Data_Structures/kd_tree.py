import pandas as pd
import numpy as np
from sklearn.neighbors import KDTree as sk_KDTree
import matplotlib.pyplot as plt
import sys
import os
import hashlib

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class KDTree:
    def __init__(self, points, reviews, country_keys):
        self.tree = None
        self.points = points
        self.reviews = reviews  # Store reviews for reference
        self.country_keys = country_keys  # 4 digit hex hash of the country
        self.build(points)

    def build(self, points):
        """
        Build the KD-Tree using sklearn.
        "points" should be a 2D numpy array
        """
        self.tree = sk_KDTree(points)

    def add_point(self, new_point, new_review, new_country):
        """
        Add a new point, review and country to the KD-Tree.

        Args:
            new_point (list or array-like): The new point to add [review_date, rating, price].
            new_review (str): The associated review for the new point.
            new_country (str): The country of origin for the new point.

        Returns:
            None: The KD-Tree is rebuilt with the updated data.
        """
        # Append the new point and review to the existing data
        self.points = np.vstack([self.points, new_point])
        self.reviews = np.append(self.reviews, new_review)

        # Hash the country
        new_country_key = hashlib.sha1(new_country.encode()).hexdigest()[-4:]
        self.country_keys = np.append(self.country_keys, new_country_key)

        # Rebuild the KD-Tree with the updated points
        self.build(self.points)

        # print(f"\nAdded new point: {new_point} with review: {new_review}")

    def delete_points(self, country_key):
        """
        Delete points from the KD-Tree based on their country key.

        Args:
            country_key (str): Hashed country.

        Returns:
            None: The KD-Tree is rebuilt with the updated data.
        """
        # Find the indices of the points to delete
        indices_to_delete = []
        for idx, key in enumerate(self.country_keys):
            if key == country_key:
                indices_to_delete.append(idx)

        # Remove the points and reviews
        self.points = np.delete(self.points, indices_to_delete, axis=0)
        self.reviews = np.delete(self.reviews, indices_to_delete)
        self.country_keys = np.delete(self.country_keys, indices_to_delete)

        # Rebuild the KD-Tree with the updated points
        self.build(self.points)

        print(f"\nDeleted {len(indices_to_delete)} points with country key: {country_key}")

    def search(self, lower_bounds, upper_bounds):
        """
        Search for points within the given bounds for all axes using the KD-Tree.

        Args:
            lower_bounds (list): Lower bounds for each axis [review_date, rating, price].
            upper_bounds (list): Upper bounds for each axis [review_date, rating, price].

        Returns:
            list: Points and their associated reviews within the specified range.
        """
        if len(lower_bounds) != 3 or len(upper_bounds) != 3:
            raise ValueError("Bounds must have exactly three values for the three axes.")

        # Compute the midpoint and radius for the search
        center = [(low + high) / 2 for low, high in zip(lower_bounds, upper_bounds)]
        radius = max((high - low) / 2 for low, high in zip(lower_bounds, upper_bounds))

        # Query points within the hypersphere defined by center and radius
        indices = self.tree.query_radius([center], r=radius)[0]

        # Filter results within the actual range bounds
        matching_points = []
        matching_reviews = []

        for idx in indices:
            point = self.points[idx]
            if all(lower_bounds[i] <= point[i] <= upper_bounds[i] for i in range(3)):
                matching_points.append(point)
                matching_reviews.append(self.reviews[idx])

        # Convert to NumPy arrays
        matching_points = np.array(matching_points)  # 2D array with cols=3
        matching_reviews = np.array(matching_reviews)  # 1D array

        return matching_points, matching_reviews

    def print_search_results(self, matching_points, matching_reviews):
        """Prints the search results."""
        # Print results
        print(f"\nFound {len(matching_points)} points within the specified ranges:")
        for point, review in zip(matching_points, matching_reviews):
            print(f"\nPoint: {point}\nReview: {review}")

    def visualize(self, points, reviews):
        """
        Visualize the points in a 3D scatter plot.
        """
        fig = plt.figure(figsize=(10, 7))
        ax = fig.add_subplot(111, projection="3d")

        # Scatter plot the points
        ax.scatter(points[:, 0], points[:, 1], points[:, 2], c="blue", marker="o", picker=True)

        # Label axes
        ax.set_xlabel("Review Date (Year)")
        ax.set_ylabel("Rating")
        ax.set_zlabel("Price (100g USD)")

        # Add title
        ax.set_title("3D Scatter Plot of Coffee Review Points")

        def on_pick(event):
            """Handle the pick event to display the associated review."""
            ind = event.ind[0]  # Index of the picked point
            review = reviews[ind]
            print(f"\nReview for selected point ({points[ind]}):\n{review}")

        # Connect the pick event
        fig.canvas.mpl_connect("pick_event", on_pick)

        plt.show()


# Example usage
if __name__ == "__main__":
    # Load CSV file
    csv_file = "../Coffee_Reviews_Dataset/simplified_coffee.csv"
    df = pd.read_csv(csv_file)

    # Keep only the date from the review_date column
    df["review_date"] = pd.to_datetime(df["review_date"], format="%B %Y").dt.year

    # Extract the 3D data: review_date, rating, 100g_USD
    points = df[["review_date", "rating", "100g_USD"]].to_numpy()

    # Extract the reviews
    reviews = df["review"].to_numpy()

    # Extract the countries
    countries = df["loc_country"].to_numpy()

    # Hash the countries
    country_keys = [hashlib.sha1(country.encode()).hexdigest()[-4:] for country in countries]

    # Build the KD-Tree
    kd_tree = KDTree(points, reviews, country_keys)

    # kd_tree.visualize(points, reviews)

    new_point = [2018, 94, 5.5]  # Example new point
    new_review = "A rich and vibrant coffee with hints of fruit and chocolate."
    new_country = "United States"

    kd_tree.add_point(new_point, new_review, new_country)

    # Search for points within a specific range
    lower_bounds = [2017, 90, 4.0]
    upper_bounds = [2018, 95, 5.5]

    points, reviews = kd_tree.search(lower_bounds, upper_bounds)  # 88 points result
    kd_tree.print_search_results(points, reviews)

    # Delete points form the United States
    country_key = hashlib.sha1("United States".encode()).hexdigest()[-4:]
    kd_tree.delete_points(country_key)

    points, reviews = kd_tree.search(lower_bounds, upper_bounds)  # 88 points result

    kd_tree.print_search_results(points, reviews)
