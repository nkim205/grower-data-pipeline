from pipeline.base import Component, DataWrapper
from pipeline.components.standardize import Standardize
import os
import pandas as pd
from datetime import timedelta

class Processing(Component):
    def __init__(self, name):
        super().__init__(name)
        
        self.schema = [
            'ID', 
            'county', 
            'daily_max_customers_affected', 
            'per_outage_customers_affected', 
            'customers_served',
            'start_time',
            'end_time',
            'duration'
        ]
        
        self.state = None
        self.date = None
        self.std = None
        self.col_map = None
        self.col_lists = None
        self.county_map = None
        self.raw_county_list = None
        self.master_county_list = None
        self.county_dfs = None

    def aggregate(self, df, county):
        df = df[['county', 'per_outage_customers_affected', 'customers_served', 'timestamp']].copy()

        df = df.sort_values('timestamp')
        last_id = self.county_dfs[county]['ID'].max() if not self.county_dfs[county].empty else 0
        threshold = timedelta(hours=1, minutes=14)

        df['diff'] = df['timestamp'].diff()
        mask = df['diff'] > threshold
        df['new_outage'] = (df['diff'].isna() | mask)
        df['ID'] = df['new_outage'].cumsum() + last_id

        result = (
            df.groupby('ID').agg(
                county=('county', 'first'),
                per_outage_customers_affected=('per_outage_customers_affected', 'max'),
                customers_served=('customers_served', 'max'),
                start_time=('timestamp', 'min'),
                end_time=('timestamp', 'max')
            ).reset_index()
        )

        result['daily_max_customers_affected'] = 0
        result['duration'] = result['end_time'] - result['start_time']

        if self.county_dfs[county].empty:
            self.county_dfs[county] = result
        else:
            self.county_dfs[county] = pd.concat([self.county_dfs[county], result], ignore_index=True)

    def fill_daily_max(self, county):
        self.county_dfs[county]['per_outage_customers_affected'] = pd.to_numeric(
            self.county_dfs[county]['per_outage_customers_affected'],
            errors='coerce'
        )

        if self.county_dfs[county]['per_outage_customers_affected'].sum() > 0:
            daily_max = self.county_dfs[county]['per_outage_customers_affected'].max()
            self.county_dfs[county]['daily_max_customers_affected'] = daily_max

    def create_filler(self, county):
        read_path = os.path.join(self.std.base_path, 'historical.csv')
        historical_val = -1
        
        try:
            df = pd.read_csv(read_path)
            county_df = df.loc[df['county'] == county]
            
            if not county_df.empty:
                historical_val = int(county_df['customers_served'].sum())
            else:
                print(f"Unable to find historical data for {county}, {self.state}")
        except FileNotFoundError:
            print(f"Unable to find historical.csv for {self.state}")

        result = pd.DataFrame([{
            "ID": 1,
            "county": county,
            "daily_max_customers_affected": 0,
            "per_outage_customers_affected": 0,
            "customers_served": historical_val,
            "start_time": pd.NaT,
            "end_time": pd.NaT,
            "duration": pd.Timedelta(0)
        }], columns=self.schema)

        self.county_dfs[county] = result
        self.county_dfs[county] = self.county_dfs[county].reindex(columns=self.schema)

    def aggregate_customers_served(self, county):
        self.county_dfs[county]['customers_served'] = pd.to_numeric(
            self.county_dfs[county]['customers_served'],
            errors='coerce'
        ).fillna(0)

        unique_customers_served = self.county_dfs[county]['customers_served'].drop_duplicates()
        total = int(unique_customers_served.sum())
        self.county_dfs[county]['customers_served'] = total

    def process(self, data):
        for i, df in enumerate(data):
            df.columns = df.columns.str.strip().str.lower()
            res = self.std.standardize(df)

            if res[0]:
                df = res[1]
            else:
                print("The following columns were missing: ")
                print(res[1])
                continue

            for county in self.raw_county_list:
                county_df = df[df['county'] == county]

                if county_df.size != 0:
                    self.aggregate(county_df, county)

        for county in self.county_dfs:
            if not self.county_dfs[county].empty:
                self.fill_daily_max(county)

        for county in self.master_county_list:
            if self.county_dfs[county].empty:
                self.create_filler(county)

        for county in self.county_dfs:
            if not self.county_dfs[county].empty:
                self.aggregate_customers_served(county)

        return self.county_dfs

    def run(self, data):
        if not data or len(data) == 0:
            print("Warning: No data received")
            return DataWrapper(pd.DataFrame(columns=self.schema))
        
        first_df = data[0]
        
        if 'state' in first_df.columns:
            self.state = first_df['state'].iloc[0].upper() if len(first_df) > 0 else 'AL'
        else:
            print("Warning: State not found in data, defaulting to 'AL'")
            self.state = 'AL'
        
        if 'timestamp' in first_df.columns:
            first_timestamp = pd.to_datetime(first_df['timestamp'].iloc[0]) if len(first_df) > 0 else pd.Timestamp.now()
            self.date = first_timestamp.strftime("%Y-%m-%d")
        else:
            self.date = pd.Timestamp.now().strftime("%Y-%m-%d")
            print(f"Warning: Timestamp not found in data, using today's date: {self.date}")
        
        print(f"Starting processing for {self.state} on {self.date}")
        
        self.std = Standardize(name="Standardize", state=self.state, date=self.date)
        
        self.col_map = self.std.col_map
        self.col_lists = self.std.col_lists
        self.county_map = self.std.county_map
        self.raw_county_list = self.std.raw_county_list
        self.master_county_list = self.std.master_county_list
        
        self.county_dfs = {c: pd.DataFrame(columns=self.schema) for c in self.master_county_list}
        
        processed_data = self.process(data)
        
        combined = pd.concat(processed_data.values(), ignore_index=True)
        
        print(f"Processing complete for {self.state}")
        
        per_county_data = DataWrapper(combined)
        return per_county_data