"""
Enrich Texas education data with Census ACS median household income by county.
Uses Census Bureau API (2022 ACS 5-year estimates).
"""

import requests
import pandas as pd

# 1. Fetch Census county income data for Texas
print("Fetching Census ACS county income data...")
url = "https://api.census.gov/data/2022/acs/acs5"
params = {
    "get": "NAME,B19013_001E",
    "for": "county:*",
    "in": "state:48",
}
r = requests.get(url, params=params)
r.raise_for_status()

data = r.json()
census_df = pd.DataFrame(data[1:], columns=data[0])
census_df["median_household_income"] = census_df["B19013_001E"].astype(float)

# Extract county name for join (e.g., "Anderson County, Texas" -> "ANDERSON")
census_df["countyName"] = (
    census_df["NAME"].str.replace(" County, Texas", "", case=False).str.upper()
)
census_df = census_df[["countyName", "median_household_income"]]

# Handle -666666666 (Census suppression code for small areas)
census_df = census_df.replace(-666666666, float("nan"))

print(f"  Loaded income for {len(census_df)} Texas counties")

# 2. Load education data
print("Loading Texas education data...")
df = pd.read_csv("texasdata.csv")

# 3. Merge on county name (TEA county numbers differ from Census FIPS)
df["countyName"] = df["countyName"].str.upper()
df = df.merge(census_df, on="countyName", how="left")

# 6. Save enriched data
output_path = "texasdata_enriched.csv"
df.to_csv(output_path, index=False)

print(f"Saved enriched data to {output_path}")
print(f"  Shape: {df.shape}")
print(f"  New column: median_household_income")
print(f"  Rows with income: {df['median_household_income'].notna().sum()} / {len(df)}")
