from pipeline.base import Component, DataWrapper
import os
import csv
import pandas as pd
from rapidfuzz import process

class Preprocess(Component):
    def __init__(self, name, state):
        super().__init__(name)
        self.state = state
        self.base_path = os.path.join("pipeline", "mappings", f"{self.state}")
        os.makedirs(self.base_path, exist_ok=True)

    def writeRawColNames(self, data):
        # NOTE: original to standarized column name mappings are done manually using this csv
        raw_cols = set()
        for col in data.columns:
            raw_cols.add(col.strip().lower())

        output_path = os.path.join(self.base_path, "raw_col_names.txt")
        with open(output_path, "w") as f:
            for c in sorted(raw_cols):
                f.write(c + "\n")

    def buildRawCountySet(self, data, county_col_list):
        raw_county_set = set()

        # Iterate through all columns to get raw county name list
        for col in data.columns:
            if col in county_col_list:
                # Filter to only use rows where key column != 'muni'
                if 'key' in data.columns:
                    mask = data['key'].str.lower().str.strip() != 'muni'

                    values_to_add = (
                        data.loc[mask, col]
                        .dropna()
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .replace("", pd.NA)
                        .dropna()
                    )
                    raw_county_set.update(values_to_add)
                else:
                    values_to_add = (
                        data[col]
                        .dropna()
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .replace("", pd.NA)
                        .dropna()
                    )
                    raw_county_set.update(values_to_add)
        
        raw_county_set = sorted(raw_county_set)
        output_path = os.path.join(self.base_path, "raw_county_list.txt")
        with open(output_path, "w") as f:
            for c in raw_county_set:
                f.write(c + "\n")

    def buildCountyMap(self):
        # Builds the mapping of {original name : standardized name} and stores it in a csv
        
        # Get the list of raw county names
        raw_county_list = []
        read_path = os.path.join(self.base_path, "raw_county_list.txt")
        try:
            with open(read_path, "r") as f:
                raw_county_list = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            print(f"Error: {read_path} not found. Run buildRawCountySet first.")
            return

        # Get the list of standardized names we will use
        master_county_set = []
        read_path = os.path.join(self.base_path, "master_county_list.txt")
        try:
            with open(read_path, "r") as f:
                master_county_set = [
                    line.strip().lower().replace("county", "").strip() 
                    for line in f if line.strip()
                ]
        except FileNotFoundError:
            print(f"Error: {read_path} not found. Please create master_county_list.txt manually.")
            return
        
        # Fuzzy match raw county names to master county names
        county_map = {}  # {raw : [standardized name, score]}
        for raw in raw_county_list:
            match, score, _ = process.extractOne(raw, master_county_set)
            if score >= 85:
                county_map[raw] = [match, score]

        # Write results to csv
        output_path = os.path.join(self.base_path, "county_map.csv")
        rows = [(raw, vals[0], vals[1]) for raw, vals in county_map.items()]
        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["raw", "standard", "score"])
            writer.writerows(rows)

    def preprocess(self, data, county_col_list=None):
        # NOTE: some sub-routines rely on the manually built column mapping to be completed
        data.columns = [col.strip().lower() for col in data.columns]
        
        if county_col_list is None:
            county_col_list = ['countynam', 'countyname', 'county', 'counties', 
                             'area', 'name', 'area_name']
        
        self.writeRawColNames(data)
        self.buildRawCountySet(data, county_col_list)
        self.buildCountyMap()

        return None
    
    def run(self, data):
        # this is the main function to be implemented, use your helper functions here 
        normalized_data = self.preprocess(data)
        # once you have data ready for the next step, now we wrap it using the DataWrapper Class
        per_county_data = DataWrapper(normalized_data)
        # we can return this data to the next component of the pipeline
        return per_county_data