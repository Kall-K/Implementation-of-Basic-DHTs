import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

from kd_tree import KDTree
from lsh import LSH


"""
Example usage of KD-Tree and LSH for multidimensional data.
Using the Coffee Reviews Dataset, we build a KD-Tree to store the 3D data points and perform range searches.
Then we preprocess the reviews using TF-IDF and build an LSH index to find the N similar reviews.
"""


"""-----------KD-Tree------------"""
# Load CSV file
csv_file = "Coffee_Reviews_Dataset/simplified_coffee.csv"
df = pd.read_csv(csv_file)

# Keep only the date from the review_date column
df["review_date"] = pd.to_datetime(df["review_date"], format="%B %Y").dt.year

# Extract the 3D data: review_date, rating, 100g_USD
points = df[["review_date", "rating", "100g_USD"]].to_numpy()

# Extract the reviews
reviews = df["review"].to_numpy()

# Build the KD-Tree
kd_tree = KDTree(points, reviews)

# Optionallt visualize the 3D points
# kd_tree.visualize(points, reviews)

# Optionally a new points can be added
new_point = [2018, 94, 5.5]  # Example new point
new_review = "Very delicate and sweet. Lemon verbena, dried persimmon, dogwood, baker's chocolate in aroma and cup. Balanced, sweet-savory structure; velvety-smooth mouthfeel. The sweetly herb-toned finish centers on notes of lemon verbena and dried persimmon wrapped in baker's chocolate."

kd_tree.add_point(new_point, new_review)

# Search for points within a specific range [date, rating, price]
lower_bounds = [2017, 90, 4.0]
upper_bounds = [2018, 95, 5.5]

points, reviews = kd_tree.search(lower_bounds, upper_bounds)


"""-------------LSH--------------"""
# Preprocess and vectorize the reviews
vectorizer = TfidfVectorizer()
doc_vectors = vectorizer.fit_transform(reviews).toarray()

# Initialize and populate LSH
lsh = LSH(num_bands=4, num_rows=5)
for vector in doc_vectors:
    lsh.add_document(vector)

# Find the top 5 most similar pairs
N = 5
similar_pairs = lsh.find_similar_pairs(N)

# Display a list of the top N most similar reviews
similar_docs = lsh.find_similar_docs(similar_pairs, reviews, N)

print(f"\nThe {N} Most Similar Reviews:\n")
for i, doc in enumerate(similar_docs, 1):
    print(f"{i}. {doc}\n")