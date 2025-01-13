import pandas as pd

# Load the dataset
file_path = "simplified_coffee.csv"
df = pd.read_csv(file_path)

# Count the unique loc_countries
unique_countries = df["loc_country"].nunique()

print(f"There are {unique_countries} different loc_countries in the dataset.")
# Answer: There are 12 different loc_countries in the dataset.
