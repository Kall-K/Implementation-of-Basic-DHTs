import pandas as pd
import hashlib


def hash_key(value):
    """
    Hash the input value and return the last 4 hex digits.
    """
    sha1_hash = hashlib.sha1(value.encode()).hexdigest()
    return sha1_hash[-4:]


# Load the dataset
file_path = "simplified_coffee.csv"
df = pd.read_csv(file_path)

# Unique County Names and their Hashes
unique_countries = df["loc_country"].unique()
hashes = [hash_key(country) for country in unique_countries]


# Count the unique loc_countries
num_unique_countries = df["loc_country"].nunique()

print(f"There are {num_unique_countries} different loc_countries in the dataset.")
print("Unique Countries and their Hashes:")
for country, hash_value in zip(unique_countries, hashes):
    print(f"{country}: {hash_value}")

"""
There are 12 different loc_countries in the dataset.
Unique Countries and their Hashes:
United States: 372b
Canada: 28ad
Hong Kong: 46c5
Hawai'i: 6073
Taiwan: 17a5
England: 4ca4
Australia: a26f
Guatemala: dc5e
Japan: 817b
China: 79a3
Kenya: 87c4
New Taiwan: e74e
"""
