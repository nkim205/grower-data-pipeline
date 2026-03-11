from pipeline.base import Component, DataWrapper
import pandas as pd
import os
import csv

class Cleaner(Component):
    def __init__(self, name: str, state, date):
        super().__init__(name)
        self.state = state
        self.date = date
        self.fips_path = os.path.join("pipeline", "mappings", "cleaned_FIPS.csv")
        self.fips = pd.read_csv(self.fips_path, dtype=str)
        # Keep only rows for the target state
        self.fips = self.fips[self.fips["state_abbr"].str.lower() == self.state]
        # Standardize FIPS CSV county names 
        self.fips["county_name_clean"] = self.fips["county_name"].str.strip().str.lower()



    def merge(self, data):
        data["county_clean"] = data["county"].str.strip().str.lower()
        
        # Merge metrics output with FIPS file
        merged = pd.merge(
            data,
            self.fips,
            left_on="county_clean",
            right_on="county_name_clean",
            how="left"
        )

        return merged



    def get_final(self, data, type):
        final = pd.DataFrame({
            "County": data["county"].str.title(),
            "FIPS": data["fips"],
            "SAIDI": data["saidi"],
            "LOWER_SAIFI": data["lower_saifi"],
            "SAIFI": data["middle_saifi"],  # middle_saifi is SAIFI
            "UPPER_SAIFI": data["upper_saifi"],
            "State": data["state_name"].str.title(),
            "Date": self.date
        })
        
        return final


    
    def run(self, data: DataWrapper) -> DataWrapper:
        raw_export = self.get_final(data=self.merge(data.data[0]), type="raw")
        dashboard_export = self.get_final(data=self.merge(data.data[1]), type="db")
        dfs = [raw_export, dashboard_export]

        return DataWrapper(
            data=dfs, 
            metadata={"s3_prefix": self.state.lower().strip()}
        )
        