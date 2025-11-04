from pipeline.base import Component, DataWrapper
import os
import csv
import pandas as pd
from rapidfuzz import process

class Preprocess(Component):
    def __init__(self, name, state):
        super.__init__(name)
        self.state = state
        self.base_path = os.path.join("pipeline\\mappings", f"{self.state}")
        # initialze everything else

    def writeRawColNames(self, data):
        # NOTE: original to standarized column name mappings are done manually using this csv
        raw_cols = set()
        for col in data.columns:
            raw_cols.update(col.strip().lower())

        output_path = os.path.join(self.base_path, "raw_col_names.txt")
        with open(output_path, "w") as f:
            for c in raw_cols:
                f.write(c + "\n")

    def buildRawCountySet(self, data):
        # Get a list of which columns to search county names for
        all_mappings = pd.read_csv(self.base_path, "col_map.csv")
        county_col_list = all_mappings["county"].tolist()
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
                        data.loc[col]
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

    def buildCountyMap(self, data):
        # Builds the mapping of {original name : standardized name} and stores it in a csv
        
        # Get the list of raw county names
        raw_county_list = set()
        read_path = os.path.join(self.base_pathm, "raw_county_list.txt")
        with open(read_path, "r") as f:
            lines = f.readlines()
            for line in lines:
                raw_county_list.update(line)

        # Get the list of standardized names we will use
        master_county_set = set()
        read_path = os.path.join(self.base_path, "master_county_list.txt")
        with open(read_path, "r") as f:
            lines = f.readlines()
            for line in lines:
                master_county_set.update(line.lower().replace("county", "").strip())
        
        # Fuzzy match raw county names to master county names
        county_map = {} # {raw : [standardized name, score]}
        for raw in raw_county_list:
            match, score, _ = process.extractOne(raw, master_county_set)
            if score >= 85:
                county_map[raw] = [match, score]

        # Write results to csv
        output_path = os.path.join(self.base_path, "county_map.csv")
        rows = [(raw, vals[0], vals[1]) for raw, vals in county_map.items()]
        with open(output_path, "w", newlines="") as f:
            writer = csv.writer(f)
            writer.writerow("raw", "standard", "score")
            writer.writerows(rows)

    def preprocess(self, data):
        # Uncomment preprocessing calls to run
        # NOTE: some sub-routines rely on the manually built column mapping to be completed
        data.columns = [col.strip().lower() for col in data.columns]
        self.writeRawColNames(data)
        self.buildRawCountySet(data)
        self.buildCountyMap(data)

        return None
    
    def run(self, data):
        # this is the main function to be implemented, use your helper functions here 
        normalized_data = self.preprocess(data)
        # once you have data ready for the next step, now we wrap it using the DataWrapper Class
        per_county_data = DataWrapper(normalized_data)
        # we can return this data to the next component of the pipeline
        return per_county_data