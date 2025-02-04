import pandas as pd
import numpy as np
from sklearn.neighbors import KDTree as sk_KDTree
import matplotlib.ticker as ticker
import tkinter as tk
import sys
import os
import hashlib

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class KDTree:
    def __init__(self, points, reviews, country_keys, countries=None):
        """
        Initialize the KDTree with points, reviews, country keys, and a list of original countries.

        Args:
            points (numpy array): Array of data points.
            reviews (numpy array): Array of reviews.
            country_keys (numpy array): Array of hashed country keys.
            countries (list, optional): List of original country names. Defaults to an empty list.
        """
        self.tree = None
        self.points = points
        self.reviews = reviews  # Store reviews for reference
        self.country_keys = country_keys  # 4-digit hex hash of the country
        self.countries = countries if countries is not None else []  # Store original country names
        self.build(points)

    def build(self, points):
        """
        Build the KD-Tree using sklearn.
        """
        self.tree = sk_KDTree(points)

    def add_point(self, new_point, new_review, new_country):
        """
        Add a new point, review, and country to the KD-Tree.

        Args:
            new_point (list or array-like): The new point to add [review_date, rating, price].
            new_review (str): The associated review for the new point.
            new_country (str): The country of origin for the new point.
        """
        # Append the new point and review to the existing data
        self.points = np.vstack([self.points, new_point])
        self.reviews = np.append(self.reviews, new_review)

        # Hash the country and append to country_keys
        new_country_key = hashlib.sha1(new_country.encode()).hexdigest()[-4:]
        self.country_keys = np.append(self.country_keys, new_country_key)

        # Append the original country to the countries list
        self.countries = np.append(self.countries, new_country)

        # Rebuild the KD-Tree with the updated points
        self.build(self.points)

    def delete_points(self, country_key):
        """
        Delete points from the KD-Tree based on their country key.

        Args:
            country_key (str): Hashed country.
        """
        # Find the indices of the points to delete
        indices_to_delete = [idx for idx, key in enumerate(self.country_keys) if key == country_key]

        if not indices_to_delete:
            print(f"No points found with country key: {country_key}")
            return

        # Remove the points, reviews, country_keys, and countries
        self.points = np.delete(self.points, indices_to_delete, axis=0)
        self.reviews = np.delete(self.reviews, indices_to_delete)
        self.country_keys = np.delete(self.country_keys, indices_to_delete)
        self.countries = np.delete(self.countries, indices_to_delete)

        # Rebuild the KD-Tree with the updated points
        if self.points.size > 0:
            self.build(self.points)
        else:
            self.tree = None

        print(f"Deleted {len(indices_to_delete)} points with country key: {country_key}\n")

    def print_countries(self):
        """
        Print the list of countries stored in the KDTree.
        """
        print("Countries in KDTree:", self.countries)

    def update_points(self, country_key=None, criteria=None, update_fields=None):
        """
        Update points or reviews in the KD-Tree with flexible criteria.

        Args:
            country_key (str, optional): The hashed key of the country. If None, updates all countries.
            criteria (dict, optional): Additional filters for specific attributes.
                Example: {"review_date": 2017, "rating": 90}
            update_fields (dict): Dictionary specifying what to update. For example:
                {"point": [new_review_date, new_rating, new_price], "review": "New review text",
                "attributes": {"rating": 95}}

        Returns:
            int: Number of updates applied.
        """
        if update_fields is None:
            print("No update fields provided. Aborting update.")
            return 0

        # Find indices of points to update based on the criteria
        indices_to_update = []
        for idx, (point, country_key_entry) in enumerate(zip(self.points, self.country_keys)):
            # Match country key if provided
            if country_key and country_key_entry != country_key:
                continue

            # Match additional criteria if provided
            if criteria:
                match = all(
                    point[CRITERIA_MAPPING[key]] == value for key, value in criteria.items()
                )
                if not match:
                    continue

            indices_to_update.append(idx)

        updates_applied = 0

        for idx in indices_to_update:
            # Update the point if specified
            if "point" in update_fields:
                print(f"Updating point at index {idx}:")
                print(f"Old Point: {self.points[idx]}")
                self.points[idx] = update_fields["point"]
                print(f"New Point: {self.points[idx]}")

            # Update specific attributes of the point
            if "attributes" in update_fields:
                print(f"Updating attributes of point at index {idx}:")
                for attr_key, attr_value in update_fields["attributes"].items():
                    point_idx = CRITERIA_MAPPING[attr_key]
                    print(f" - Old {attr_key}: {self.points[idx][point_idx]}")
                    self.points[idx][point_idx] = attr_value
                    print(f" - New {attr_key}: {self.points[idx][point_idx]}")

            # Update the review if specified
            if "review" in update_fields:
                print(f"Updating review at index {idx}:")
                print(f"Old Review: {self.reviews[idx]}")
                self.reviews[idx] = update_fields["review"]
                print(f"New Review: {self.reviews[idx]}")

            updates_applied += 1

        # Rebuild the KD-Tree if any points were updated
        if updates_applied > 0 and ("point" in update_fields or "attributes" in update_fields):
            self.build(self.points)

        if updates_applied == 0:
            print("No matching points found for the update criteria.")
        else:
            print(f"Applied {updates_applied} updates.")

        return updates_applied

    def search(self, country_key, lower_bounds, upper_bounds):
        """
        Search for points of a given country key within the given bounds for all axes using the KD-Tree.

        Args:
            country_key (str): Hashed country key.
            lower_bounds (list): Lower bounds for each axis [review_date, rating, price].
                                Use `None` for axes that should not be constrained.
            upper_bounds (list): Upper bounds for each axis [review_date, rating, price].
                                Use `None` for axes that should not be constrained.

        Returns:
            list: Points and their associated reviews within the specified range.
        """
        if len(lower_bounds) != 3 or len(upper_bounds) != 3:
            raise ValueError("Bounds must have exactly three values for the three axes.")

        # Compute the midpoint and radius for the search
        # Only consider axes where both lower and upper bounds are not None
        valid_axes = [
            i for i in range(3) if lower_bounds[i] is not None and upper_bounds[i] is not None
        ]

        # Calculate center and radius only for valid axes
        for i in range(3):
            if i not in valid_axes:
                lower_bounds[i] = np.min(self.points[:, i])
                upper_bounds[i] = np.max(self.points[:, i])
        center = [(lower_bounds[i] + upper_bounds[i]) / 2 for i in range(3)]
        radius = np.linalg.norm((np.array(upper_bounds) - np.array(lower_bounds)) / 2)

        # Query points within the hypersphere defined by center and radius + a small epsilon
        indices = self.tree.query_radius([center], r=radius + 1e-8)[0]

        # Filter results within the actual range bounds and matching the country_key
        matching_points = []
        matching_reviews = []

        for idx in indices:
            point = self.points[idx]
            if self.country_keys[idx] == country_key and all(
                (lower_bounds[i] is None or lower_bounds[i] <= point[i])
                and (upper_bounds[i] is None or point[i] <= upper_bounds[i])
                for i in range(3)
            ):
                matching_points.append(point)
                matching_reviews.append(self.reviews[idx])

        # Convert to NumPy arrays
        matching_points = np.array(matching_points)
        matching_reviews = np.array(matching_reviews)

        return matching_points, matching_reviews

    def get_unique_country_keys(self):
        """Return tuple of lists with the unique country keys and their assosiated countries."""
        unique_country_keys = np.unique(self.country_keys)
        unique_countries = []
        for key in unique_country_keys:
            for country in self.countries:
                if key == hashlib.sha1(country.encode()).hexdigest()[-4:]:
                    unique_countries.append(country)
                    break
        return list(unique_country_keys), unique_countries

    def get_points(self, country_key):
        """Return the points and reviews for a specific country key."""
        points = []
        reviews = []
        for idx, key in enumerate(self.country_keys):
            if key == country_key:
                points.append(self.points[idx].tolist())
                reviews.append(self.reviews[idx].tolist())
        return np.array(points), np.array(reviews)

    def print_search_results(self, matching_points, matching_reviews):
        """Prints the search results, including the associated country."""
        print(f"\nFound {len(matching_points)} points within the specified ranges:")
        for point, review in zip(matching_points, matching_reviews):
            # Find the index of the matching point
            index = np.where((self.points == point).all(axis=1))[0][0]
            # Retrieve the country key and reverse lookup to get the country name
            country_key = self.country_keys[index]
            country = next(
                (
                    country
                    for country, key in zip(
                        countries,
                        [hashlib.sha1(country.encode()).hexdigest()[-4:] for country in countries],
                    )
                    if key == country_key
                ),
                "Unknown",
            )
            print(f"\nPoint: {point}\nReview: {review}\nCountry: {country}")

    def visualize(
        self, ax, canvas, points=None, reviews=None, country_key=None, country=None, title=None
    ):
        """
        Visualize the points in a 3D scatter plot on the provided Axes object.
        If points and reviews are None, visualize all stored points.

        Args:
            ax (Axes): The Matplotlib Axes object to plot on.
            canvas (FigureCanvasTkAgg): The Tkinter canvas for displaying the plot.
            points (numpy array, optional): Array of data points. If None, use all stored points.
            reviews (numpy array, optional): Array of reviews. If None, use all stored reviews.
        """
        # If to points and reviews are provided, use all stored points and reviews
        if points is None or reviews is None:
            if self.points is None or self.reviews is None or len(self.points) == 0:
                print("No points available for visualization.")
                return
            points = self.points
            reviews = self.reviews

        if len(points) > 0:
            # Create a 3D scatter plot if points are available
            ax.scatter(points[:, 0], points[:, 1], points[:, 2], c="blue", marker="o", picker=True)

        # Label axes
        ax.set_xlabel("Review Date (Year)")
        ax.set_ylabel("Rating")
        ax.set_zlabel("Price (100g USD)")

        # Set integer year ticks
        ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))

        # If there's only one point or the all the points have the same year, narrow the x-axis range
        if len(points) > 0 and (points.shape[0] == 1 or np.all(points[:, 0] == points[0, 0])):
            year = points[0, 0]
            ax.set_xlim(year - 1, year + 1)  # 1 year margin on each side
            ax.set_xticks([year])

        # Add title
        if title is not None:
            ax.set_title(title)
        elif country_key is None or country is None:
            ax.set_title("3D Scatter Plot of Coffee Review Points")
        else:
            ax.set_title(
                f"3D Scatter Plot of Coffee Review Points from {country} - Key: {country_key}"
            )

        # Redraw the canvas
        canvas.draw()

    def on_pick(self, event, points, reviews, review_text):
        """Handle the pick event to display the associated review."""
        ind = event.ind[0]  # Index of the picked point
        review_text.config(state=tk.NORMAL)
        review_text.delete(1.0, tk.END)

        text = f"Point: {points[ind]}\nReview: {reviews[ind]}"
        review_text.insert(tk.END, text)
        review_text.config(font=("Courier", 11), state=tk.DISABLED)


# Map criteria keys to point array indices
CRITERIA_MAPPING = {"review_date": 0, "rating": 1, "price": 2}

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
    kd_tree = KDTree(points, reviews)

    # kd_tree.visualize(points, reviews)

    new_point = [2018, 94, 5.5]  # Example new point
    new_review = "A rich and vibrant coffee with hints of fruit and chocolate."
    new_country = "United States"

    kd_tree.add_point(new_point, new_review, new_country)

    # Search for points within a specific range
    lower_bounds = [2017, 90, 4.0]
    upper_bounds = [2018, 95, 5.5]
    usa_key = hashlib.sha1("United States".encode()).hexdigest()[-4:]

    # Search for points of the United States within the specified bounds
    points, reviews = kd_tree.search(usa_key, lower_bounds, upper_bounds)  # 80 points result
    kd_tree.print_search_results(points, reviews)

    # Delete points form the United States
    kd_tree.delete_points(usa_key)

    taiwan_key = hashlib.sha1("Taiwan".encode()).hexdigest()[-4:]
    points, reviews = kd_tree.search(taiwan_key, lower_bounds, upper_bounds)  # 9 points result
    kd_tree.print_search_results(points, reviews)

    # Update all points for Taiwan
    print("\nUpdating all points for Taiwan:\n")
    kd_tree.update_points(country_key=taiwan_key, update_fields={"attributes": {"price": 29.0}})

    # Update a specific point for Taiwan
    print("\nUpdating a specific point for Taiwan:\n")
    criteria = {"review_date": 2019, "rating": 94, "price": 29.0}
    update_fields = {"attributes": {"price": 30.0}}
    kd_tree.update_points(country_key=taiwan_key, criteria=criteria, update_fields=update_fields)

    # Update only the review for Taiwan
    print("\nUpdating only the review for Taiwan:\n")
    update_fields = {"review": "An updated review for Taiwan's coffee."}
    kd_tree.update_points(country_key=taiwan_key, update_fields=update_fields)

    # Update based on specific attributes (e.g., review_date and rating) and modify the price
    print("\nUpdating specific attributes for Taiwan:\n")
    criteria = {"review_date": 2019, "rating": 94}
    update_fields = {"attributes": {"price": 28.0}}
    kd_tree.update_points(country_key=taiwan_key, criteria=criteria, update_fields=update_fields)

    # Verify all updates by searching again
    lower_bounds = [2019, 90, 26.0]
    upper_bounds = [2020, 95, 29.0]
    points, reviews = kd_tree.search(taiwan_key, lower_bounds, upper_bounds)
    kd_tree.print_search_results(points, reviews)
    # Print all countries
    kd_tree.print_countries()
