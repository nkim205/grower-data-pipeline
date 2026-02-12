import os
import csv
import pandas as pd
from rapidfuzz import process
import pprint

class Preprocess():
    def __init__(self, state):
        self.state = state
        self.base_path = os.path.join("Preprocessing", f"{self.state}")
        # initialze everything else

    def writeRawColNames(self, data):
        # NOTE: original to standarized column name mappings are done manually using this csv
        raw_cols = set()
        for col in data.columns:
            raw_cols.add(col.strip().lower())

        output_path = os.path.join(self.base_path, "raw_col_names.txt")
        with open(output_path, "w") as f:
            for c in raw_cols:
                f.write(c + "\n")

    def buildRawCountySet(self, data):
        # Get a list of which columns to search county names for
        all_mappings = pd.read_csv(os.path.join(self.base_path, "col_map.csv"), sep=",")
        county_col_list = all_mappings.loc[
            all_mappings.iloc[:, 1].str.lower() == "county",
            all_mappings.columns[0]
        ].tolist()

        raw_county_set = set()

        # Get all matching column indicies to loop through
        indicies = [i for i, c in enumerate(data.columns) if c in county_col_list]
        
        for i in indicies:
            if 'key' in data.columns:
                series = data.iloc[:, i]
                mask = (data['key'].isna()) | (data['key'].str.lower().str.strip() != 'muni')
                non_muni = series[mask]

                values_to_add = (
                    non_muni
                    .dropna()
                    .astype(str)
                    .str.lower()
                    .str.replace("county", "", regex=True)
                    .str.strip()
                    .replace("", pd.NA)
                    .dropna()
                )
                raw_county_set.update(values_to_add)
            else:
                values_to_add = (
                    data.iloc[:, i]
                    .dropna()
                    .astype(str)
                    .str.lower()
                    .str.replace("county", "", regex=True)
                    .str.strip()
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
        raw_county_list = set()
        read_path = os.path.join(self.base_path, "raw_county_list.txt")
        with open(read_path, "r") as f:
            lines = f.readlines()
            for line in lines:
                raw_county_list.add(line.strip().lower())
        
        # Get the list of standardized names we will use
        master_county_set = set()
        read_path = os.path.join(self.base_path, "master_county_list.txt")
        with open(read_path, "r") as f:
            lines = f.readlines()
            for line in lines:
                master_county_set.add(line.lower().replace("county", "").strip())
        
        # Fuzzy match raw county names to master county names
        county_map = {} # {raw : [standardized name, score]}
        for raw in raw_county_list:
            match, score, _ = process.extractOne(raw, master_county_set)
            if score >= 85:
                county_map[raw] = [match, score]

        # Write results to csv
        output_path = os.path.join(self.base_path, "county_map.csv")
        rows = [(raw, vals[0], vals[1]) for raw, vals in county_map.items()]
        df = pd.DataFrame(rows, columns=["raw", "standard", "score"])
        csv_df = df.copy()
        csv_df.to_csv(output_path, index =False)

    def buildColLists(self):
        # Get col map
        path = os.path.join(self.base_path, "col_map.csv")
        col_map = pd.read_csv(path)
        
        # "reverse" the mappings to get the col lists then store as a new csv
        col_lists = (
           col_map
            .groupby("standard")["raw"]
            .apply(list)
            .to_dict()
        )
        df = pd.DataFrame.from_dict(col_lists, orient="index").transpose()
        df.to_csv(os.path.join(self.base_path, "raw_col_lists.csv"), index=False)

    def preprocess(self, data):
        # Uncomment preprocessing calls to run
        # NOTE: some sub-routines rely on the manually built column mapping to be completed
        data.columns = [col.strip().lower() for col in data.columns]
        self.writeRawColNames(data)
        self.buildRawCountySet(data)
        self.buildCountyMap()
        self.buildColLists()

        return None
    
state = "mt"
data = pd.read_csv(f"Preprocessing/{state.capitalize()}/{state}_outages_past_year.csv")
pre = Preprocess(state=f"{state.capitalize()}")
pre.preprocess(data)