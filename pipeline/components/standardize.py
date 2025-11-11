from pipeline.base import Component, DataWrapper
import numpy as np
import pandas as pd
import os
import csv

class Standardize(Component):
    def __init__(self, name, state, date):
        super().__init__(name)
        self.state = state
        self.date = date
        self.base_path = os.path.join("pipeline", "mappings", f"{self.state}")
        
        self.col_map = self.get_col_map()
        self.col_lists = self.get_all_col_lists()
        self.county_map = self.get_county_map()
        self.raw_county_list = self.get_raw_county_list()
        self.master_county_list = self.get_master_county_list()

    def get_col_map(self):
        col_map = {}
        read_path = os.path.join(self.base_path, "col_map.csv")
        
        try:
            with open(read_path) as f:
                reader = csv.reader(f)
                header = next(reader)
                
                for row in reader:
                    if len(row) >= 2:
                        raw = row[0].strip().lower()
                        std_name = row[1].strip().lower()
                        col_map[raw] = std_name
        except FileNotFoundError:
            print(f"Warning: col_map.csv not found at {read_path}")
        
        return col_map

    def get_all_col_lists(self):
        read_path = os.path.join(self.base_path, "raw_col_lists.csv")
        
        try:
            df = pd.read_csv(read_path)
            col_lists = {col: df[col].dropna().tolist() for col in df.columns}
        except FileNotFoundError:
            print(f"Warning: raw_col_lists.csv not found at {read_path}")
            col_lists = {}
        
        return col_lists

    def get_county_map(self):
        county_map = {}
        read_path = os.path.join(self.base_path, "county_map.csv")
        
        try:
            with open(read_path) as f:
                reader = csv.reader(f)
                header = next(reader)
                
                for row in reader:
                    if len(row) >= 2:
                        raw = row[0].strip().lower()
                        std_name = row[1].strip().lower()
                        county_map[raw] = std_name
        except FileNotFoundError:
            print(f"Warning: county_map.csv not found at {read_path}")
        
        return county_map

    def get_raw_county_list(self):
        raw_county_list = set()
        read_path = os.path.join(self.base_path, "raw_county_list.txt")
        
        try:
            with open(read_path) as f:
                for line in f:
                    cleaned = line.strip().lower()
                    if cleaned:
                        raw_county_list.add(cleaned)
        except FileNotFoundError:
            print(f"Warning: raw_county_list.txt not found at {read_path}")
        
        return raw_county_list

    def get_master_county_list(self):
        master_county_list = set()
        read_path = os.path.join(self.base_path, "master_county_list.txt")
        
        try:
            with open(read_path) as f:
                for line in f:
                    cleaned = line.strip().lower().replace("county", "").strip()
                    if cleaned:
                        master_county_list.add(cleaned)
        except FileNotFoundError:
            print(f"Warning: master_county_list.txt not found at {read_path}")
        
        return master_county_list

    def rename_cols(self, data):
        for col in data.columns:
            lower = col.strip().lower()
            
            if lower in self.col_map:
                data.rename(columns={col: self.col_map[lower]}, inplace=True)
            elif lower != "% out":
                data.drop(columns=[col], inplace=True)
        
        return data

    def handle_bad_data(self, data):
        data = data.dropna(subset=['county'])
        data = data[~data['county'].isin(['unknown', 'Unknown', 'UNKNOWN', ''])].copy()
        
        if "% out" in data.columns:
            if "customers_served" in data.columns:
                data.drop(columns="% out", inplace=True)
            else:
                data['% out'] = (
                    data['% out'].astype(str)
                    .str.replace('<', '', regex=False)
                    .str.rstrip('%')
                    .replace('', np.nan)
                )
                
                data['% out'] = pd.to_numeric(data['% out'], errors='coerce') / 100
                
                data['customers_served'] = np.where(
                    (data['% out'].notna()) & (data['% out'] != 0),
                    (data['per_outage_customers_affected'].astype(float) / data['% out']).round().astype("Int64"),
                    np.nan
                )
                
                data.drop(columns=['% out'], inplace=True)
        
        return data

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
        
        data['timestamp'] = pd.to_datetime(data['timestamp'], errors='coerce')
        data = data[data['timestamp'].dt.date == pd.to_datetime(self.date).date()]
        
        return data

    def standardize_county(self, data):
        data = data.copy()
        
        data['county'] = (
            data['county']
            .astype(str)
            .str.strip()
            .str.lower()
            .map(self.county_map)
        )
        
        data = data[data['county'].map(type) == str]
        
        return data

    def standardize(self, data):
        if 'c_affected' in self.col_lists:
            dupes = [c for c in data.columns 
                    if c.strip().lower() in self.col_lists['c_affected']]
            
            if len(dupes) > 1:
                keep = data[dupes].sum(skipna=True).idxmax()
                data.drop(
                    columns=[c for c in data.columns if c != keep and c in dupes],
                    inplace=True
                )
        
        renamed_data = self.rename_cols(data)
        good_data = self.handle_bad_data(renamed_data)
        
        std_names = ['county', 'per_outage_customers_affected', 'customers_served', 'timestamp']
        missing = [name for name in std_names if name not in good_data.columns]
        
        if missing:
            return [False, missing]
        
        type_std_data = self.standardize_data_types(good_data)
        std_data = self.standardize_county(type_std_data)
        
        return [True, std_data]

    def run(self, data):
        normalized_data = self.standardize(data)
        provider_data_std = DataWrapper(normalized_data)
        return provider_data_std