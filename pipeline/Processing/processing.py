import pandas as pd
import numpy as np
import glob
import os
import pprint
from rapidfuzz import process
from datetime import timedelta

### PREPROCESSING WORK

# Get all column names files = glob.glob('Processing\\States\\**\\*.csv')  
files = glob.glob(os.path.join('Processing\\States', '**', '*.csv'), recursive=True)
cols = set()   

for file in files:
    df = pd.read_csv(file, nrows=0)                            
    df.columns = [col.strip().lower() for col in df.columns]    
    cols.update(df.columns)

county = ['countynam', 'countyname', 'county', 'counties', 'area', 'name', 'area_name']
c_affected = ['out', 'customersaffected', '# out', 'n_out', 'aff', 'cust_a', 'customersoutnow', 'numoutages']
c_served = ['served', 'customersserved', '# served', 'premisecount', 'cust_s', 'total', 'customercount', 'count']
timestamp = ['timestamp']
col_map = {}

for name in county:
    col_map[name] = "county"
for caff in c_affected:
    col_map[caff] = "per_outage_customers_afffected"
for cser in c_served:
    col_map[cser] = "customers_served"
for time in timestamp:
    col_map[time] = "timestamp"

# Get the raw county set for each state
raw_county_dict = {}
base = os.path.join("Processing\\States")
key_val_set = set()

for state in glob.glob(os.path.join(base, '*')):
    state_code = state[-2:]
    county_set = set()

    for file in glob.glob(os.path.join(state, '*.csv')):
        df = pd.read_csv(file)
        df.columns = [col.strip().lower() for col in df.columns]
        col_name = [col for col in df.columns if col in county]
        key_col = [col for col in df.columns if col == 'key']

        # Some providers also include municipalities, this filters those out
        if 'key' in df.columns:
            mask = df['key'].str.lower() != 'muni'
            county_set.update(c.strip().lower() for c in df.loc[mask, col_name[0]].dropna())
        else:
            county_set.update(c.strip().lower() for c in df[col_name[0]].dropna())
        
    county_set = sorted(county_set)
    raw_county_dict[state_code] = county_set

# Compile the "master" county list we will use
master_county_dict = {}
base = os.path.join("Processing\\CountyList", "*.txt")

for state in glob.glob(base):
    with open(state, 'r', encoding="utf-8") as file:
        state_code = state[-6:-4]
        lines = file.readlines()
        county_list = []
        for line in lines:
            county_list.append(line.lower().replace("county", "").strip())
        master_county_dict[state_code] = county_list

# Check for different names being used for the same county
dupe_dict = {}  # Stores {State : {original name : final name}}
dupe_list = {}  # Stores {State : [original names]} 

for code in raw_county_dict:
    dupe_entry = {}
    dupe_names = []

    for raw in raw_county_dict[code]:
        match, score, _ = process.extractOne(raw, master_county_dict[code])
        if score >= 85: 
            dupe_entry[raw] = match
	    # dupe_entry.append({raw: {match: score}})
            dupe_names.append(raw)
    dupe_dict[code] = dupe_entry
    dupe_list[code] = dupe_names

# Standardizes the df columns for processing purposes
def standardize_cols(df, state):
    # Remove duplicate customers affected columns, keeping the one with the highest sum
    dupes = [c for c in df.columns if c.strip().lower() in c_affected]

    if len(dupes) > 1:
        keep = df[dupes].sum(skipna=True).idxmax()
        df.drop(columns=[c for c in df.columns if c != keep and c in dupes], inplace=True)

    # Standardize column names and remove unnecessary columns
    for col in df.columns:
        lower = col.strip().lower()

        if lower in col_map:
            df.rename(columns={col: col_map[lower]}, inplace=True)
        elif lower != "% out":
            df.drop(columns=[col], inplace=True)
    
    # Remove invalid rows in county col
    df = df.dropna(subset=['county'])
    df = df[~df['county'].isin(['unknown', 'Unknown', 'UNKNOWN', ''])].copy()

    # Calculate estimated customers served using affected / % out data
    if "% out" in df.columns:
        if "customers_served" in df.columns:
            df.drop(columns="% out", inplace=True)
        else:
            df['% out'] = df['% out'].str.replace("<", "", regex=False).str.rstrip('%').replace("", np.nan).astype(float)
            df['% out'] = pd.to_numeric(df['% out'], errors="coerce") / 100
            df['customers_served'] = np.where(
                (df['% out'].notna()) & (df['% out'] != 0), 
                ((df['per_outage_customers_afffected'].astype(float) / df['% out']).round().astype("Int64")), 
                np.nan)
            df.drop(columns=['% out'], inplace=True)

    # Check for if any columns are still missing. If so, skip this provider
    std_names = ['county', 'per_outage_customers_afffected', 'customers_served', 'timestamp']
    skip = False
    missing = []

    for name in std_names:
        if name not in df.columns:
            missing.append(name)
            skip = True
        
    if skip:
        return [False, df]  
    else:
        # Standardize customers affected and served columns (e.g. "123,456" --> 123456)
        df['per_outage_customers_afffected'] = (
            pd.to_numeric(
                df['per_outage_customers_afffected']
                    .astype(str)
                    .str.replace('"', '', regex=False)
                    .str.replace(',', '', regex=False),
                    errors='coerce'
            ).fillna(0)
        )
        
        df['customers_served'] = (
            pd.to_numeric(
                df['customers_served']
                    .astype(str)
                    .str.replace('"', '', regex=False)
                    .str.replace(',', '', regex=False),
                    errors='coerce'
            ).fillna(0)
        )
        
        # Standardize county names
        county_dict = dupe_dict[state]
        df = df.copy()
        df['county'] = df['county'].astype(str).str.strip().str.lower().map(county_dict)
        df = df[df['county'].map(type) == str]
        return [True, df]

# Pull most recent customers served data as a baseline
base = os.path.join("Processing\\States")

for state in glob.glob(os.path.join(base, '*')):
    state_code = state[-2:]
    served_dict = {}  # {county : [customers served, last updated date]}

    for file in glob.glob(os.path.join(state, '*.csv')):
        # Standardize cols 
        df = pd.read_csv(file)
        df.columns = df.columns.str.strip().str.lower()
        res = standardize_cols(df, state_code)
        if res[0]:
            df = res[1]
        else:
            continue

        # Group by county name and sort by timestamp
        df = df.sort_values(by=['county', 'timestamp'], ascending=[True, False])
        latest = df.groupby('county', as_index=False).last()
        latest['customers_served'] = pd.to_numeric(latest['customers_served'], errors='coerce')
        latest = latest[latest['customers_served'].notna() & (latest['customers_served'] >= 0)] 

        # Get the most recent customers served data for each provider
        for _, row in latest.iterrows():
            county = row['county']
            served = row['customers_served']
            timestamp = row['timestamp']
            
            if pd.notna(served):
                if county in served_dict:
                    prev_served, prev_ts = served_dict[county]
                    new_served = served + prev_served
                    new_ts = max(prev_ts, timestamp)
                    served_dict[county] = [new_served, new_ts]
                else:
                    served_dict[county] = [served, timestamp]
    
    df = pd.DataFrame.from_dict(
        served_dict,
        orient="index",
        columns=['customers_served', 'timestamp']
    ).reset_index()
    df = df.rename(columns={'index': 'county'})
    df = df.sort_values('county')
    # Uncomment below to write data into state csv
    df.to_csv(os.path.join("Processing\\CustomersServed", f"{state_code}_customers_served.csv"))

### PROCESSING 
STATE = 'AL'
DATE = "2024-05-23"

# Initialize a list of dictionaries, each entry representing a county and its data 
schema = ["ID", "county", "daily_max_customers_affected", "per_outage_customers_afffected", "customers_served", "start_time", "end_time", "duration"]
county_dfs = {c: pd.DataFrame(columns=schema) for c in master_county_dict[STATE]}
files = glob.glob(os.path.join('Processing\\States', STATE, '*.csv'))

for file in files:
    df = pd.read_csv(file)
    df.columns = df.columns.str.strip().str.lower()
    
    # Use standardize_cols function
    res = standardize_cols(df, STATE)
    if res[0]:
        df = res[1]
    else:
        continue

    # Standardize timestamp data type
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df[df['timestamp'].dt.date == pd.to_datetime(DATE).date()]
    # Sort by county name then by date and reorder columns 

    # Sort by county name then by date
    county_list = dupe_list[STATE]
    for county in county_list:
        county_df = df[df['county'] == county]
        
        if county_df.size != 0:
            county_df = county_df[['county', 'per_outage_customers_afffected', 'customers_served', 'timestamp']]
            county_df = county_df.sort_values('timestamp')

            # Group by timestamp and give IDs to each new outage
            last_id = county_dfs[county]['ID'].max() if not county_dfs[county].empty else 0
            threshold = timedelta(hours=1, minutes=14)

            county_df['diff'] = county_df['timestamp'].diff()
            mask = county_df['diff'] > threshold
            county_df['new_outage'] = (county_df['diff'].isna() | mask)
            county_df['ID'] = county_df['new_outage'].cumsum() + last_id

            result = (
                county_df.groupby('ID')
                        .agg(
                            county=('county', 'first'),
                            per_outage_customers_afffected=('per_outage_customers_afffected', 'max'),
                            customers_served=('customers_served', 'max'),
                            start_time=('timestamp', 'min'),
                            end_time=('timestamp', 'max')
                        )
                        .reset_index()
            )

            result['daily_max_customers_affected'] = 0
            result['duration'] = result['end_time'] - result['start_time']

            if county_dfs[county].empty:
                county_dfs[county] = result
            else:
                county_dfs[county] = pd.concat([county_dfs[county], result], ignore_index=True)

# Fill in daily max values
for county in county_dfs:
    if not county_dfs[county].empty:
        # Convert customers_affected to numeric
        county_dfs[county]['per_outage_customers_afffected'] = pd.to_numeric(
            county_dfs[county]['per_outage_customers_afffected'], 
            errors='coerce'
        )
        
        if county_dfs[county]['per_outage_customers_afffected'].sum() > 0:
            # Get the maximum customers_affected across ALL outages in this county for the day
            daily_max = county_dfs[county]['per_outage_customers_afffected'].max()
            # Apply this same value to all rows
            county_dfs[county]['daily_max_customers_affected'] = daily_max

# Create filler dataframes for counties with no reported outages
for county in master_county_dict[STATE]:
    if county_dfs[county].empty:

        # Pull historical customers served
        historical_base = glob.glob(os.path.join('Processing\\CustomersServed', f"{STATE}_customers_served.csv"))
        historical_val = -1
        if historical_base:
            historical_served = pd.read_csv(historical_base[0])
            historical_row = historical_served.loc[historical_served['county'] == county] 
            
            if not historical_row.empty:
                historical_val = int(row["customers_served"])
                print(f"{county} County had no reported outages. Filling in {historical_val}")
            else:
                print(f"Unable to find historical customers served data for {county}, {STATE}")
        else:
            print(f"Unable to find historical customers served data for {STATE}")

        result = pd.DataFrame([{
            "ID": 1,
            "county": county,
            "daily_max_customers_affected": 0,
            "per_outage_customers_afffected": 0,
            "customers_served": historical_val,
            "start_time": pd.NaT,
            "end_time": pd.NaT,
            "duration": pd.Timedelta(0)
        }], columns=schema) 
        county_dfs[county] = result
        
    # Reindex to reorganize column order
    county_dfs[county] = county_dfs[county].reindex(columns=schema)


# Fill in missing data for counties with no reports that day
county_customers_served = {}

# Pull most recent customers served data (WIP integration)
for county in county_dfs:
    if not county_dfs[county].empty:
        # Convert customers_served to numeric
        county_dfs[county]['customers_served'] = pd.to_numeric(
            county_dfs[county]['customers_served'], 
            errors='coerce'
        ).fillna(0)
        
        if county_dfs[county]['customers_served'].sum() > 0:
            total_served = county_dfs[county]['customers_served'].sum()
            county_customers_served[county] = total_served
            county_dfs[county]['customers_served'] = total_served
        else:
            county_customers_served[county] = 0
    else:
        county_customers_served[county] = 0

# Uncomment below to print county level data 
# pprint.pprint(county_dfs)

# Write data to CSV
combined = pd.concat(county_dfs.values(), ignore_index=True)
# Uncomment below to write data to corresponding csv file
combined.to_csv(os.path.join("Processing\\Processed_Data", f"{STATE}_all_counties_{DATE}.csv"), index=False)

num_counties = combined['county'].nunique()
print(f"Total number of counties reported for {STATE}: {num_counties}")
print(f"Processing complete for {STATE}")