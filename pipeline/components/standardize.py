from pipeline.base import Component, DataWrapper
import numpy as np
import pandas as pd

class Standardize(Component):
    def __init__(self, name, state, col_map, col_lists, dupe_dict):
        super.__init__(name)
        self.state = state
        self.col_map = col_map
        self.col_lists = col_lists
        self.dupe_dict = dupe_dict
        # initialze everything else

    # Standardizes column names and drops redundant / unnecessary columns
    def rename_cols(self, data):
        for col in data.columns:
            lower = col.strip().lower()

            if lower in self.col_map:
                data.rename(columns={col: self.col_map[lower]}, inplace=True)
            elif lower != "% out":  # % Out column requires special handling
                data.drop(columns=[col], inplace=True)
        return data
    
    # Handles back, missing, and unknown data
    def handle_bad_data(self, data):
        # Drop NULL / unknown / empty rows
        data = data.dropna(subset=['county'])
        data = data[
            ~data['county']
            .isin(['unknown', 'Unknown', 'UNKNOWN', ''])
        ].copy()
        
        # If customers served data is missing, estimate it using affected / % out
        if "% out" in data.columns:
            if "customers_served" in data.columns:
                data.drop(columns="% out", inplace=True)
            else:
                # TODO: figure out how to handle "<" more robustly
                # For a county on a given day, if all % outs are "<" estimations, pull from historical
                # If a county has at least 1 "valid" % out entry then use that for the rest of its timestamps for that day

                # FOR LATER USE (WIP)
                mask = ~data['% out'].astype(str).str.contains('<', na=True)
                valid_rows = data[mask]

                # Standardize "< xx.xx%" to "xx.xx"
                data['% out'] = (
                    data['% out'].str.replace('<', '', regex=False)
                    .str.rstrip('%').replace('', np.nan)
                    .astype(float)
                )
                # Convert decimal to percent
                data['% out'] = pd.to_numeric(data['% out'], errors='coerce') / 100
                # Calculate esimated customers served
                data['customers_served'] = np.where(
                    data['% out'].nona() & data['% out'] != 0,
                    (data['per_outage_customers_affected'].astype(float) / data['% out']).round().astype("Int64"),
                    np.nan
                )
                # Remove % out column as it is no longer needed
                data.drop(columns=['% out'], inplace=True)

        return data

    # Standardizes numeric data types
    def standardize_data_types(self, data):
        data['per_outage_customers_affected'] = (
            pd.to_numeric(
                data['per_outage_customers_affected']
                    .astype(str)
                    .str.replace('"', '', regex=False)
                    .str.replace(',', '', regex=False),
                    errors='coerce'
            ).fillna(0)
        )

        data['customers_served'] = (
            pd.to_numeric(
                data['customers_served']
                    .astype(str)
                    .str.replace('"', '', regex=False)
                    .str.replace(',', '', regex=False),
                    errors='coerce'
            ).fillna(0)
        )

        return data

    # Standardizes county names
    def standardize_county(self, data):
        county_dict = self.dupe_dict[self.state]
        data = data.copy()
        # Map raw county names to standardized names
        data['county'] = (
            data['county']
            .astype(str)
            .str.strip()
            .str.lower()
            .map(county_dict)
        )
        # Standardize datatype
        data = data[data['county'].map(type) == str]

        return data

    def standardize(self, data):
        # Check for multiple customers affected columns
        dupes = [c for c in data.columns 
                 if c.strip().lower() in self.col_lists['c_affected']] 
        
        if len(dupes) > 1:
            # If duplicate affected columns exist, keep the one with higher values
            keep = data[dupes].sum(skipna=True).idxmax()
            data.drop(
                columns=[c for c in data.columns if c != keep and c in dupes], 
                inplace=True)
        
        renamed_data = self.rename_cols(data)
        good_data = self.handle_bad_data(renamed_data)

        # Check for if any columns are still missing. If so, skip that provider
        std_names = ['county', 'per_outage_customers_afffected', 'customers_served', 'timestamp']
        skip = False
        missing = []

        for name in std_names:
            if name not in good_data.columns:
                missing.append(name)
                skip = True
        
        if skip:
            return [False, good_data]

        # Standardize data types
        type_std_data = self.standardize_data_types(good_data)
        # Standardize county names
        std_data = self.standardize_county(type_std_data)

        return std_data

    def run(self, data):
        normalized_data = self.standardize(data)
        provider_data_std = DataWrapper(normalized_data, {})
        return provider_data_std