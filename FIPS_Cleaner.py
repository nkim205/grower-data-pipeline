import pandas as pd

# load csv
input_file = "county_fips_master.csv"      
output_file = "cleaned.csv" 

df = pd.read_csv(input_file, dtype=str, encoding="latin-1")

# only keep certain columns, delete the rest
columns_to_keep = ["fips", "county_name", "state_abbr", "state_name"]
df = df[columns_to_keep]

# pad with leading 0 and remove weird spaces
df["fips"] = df["fips"].str.strip()
df["fips"] = df["fips"].apply(
    lambda x: x.zfill(5) if len(x) == 4 else x
)

# Take out "County" from county name
df["county_name"] = df["county_name"].str.replace(
    " County", "", regex=False
)

# saved cleaned file
df.to_csv(output_file, index=False)

print("done cleaning, saved to:", output_file)
