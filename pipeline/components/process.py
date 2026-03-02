from pipeline.base import Component, DataWrapper
from pipeline.components.standardize import Standardize
import os
import pandas as pd
from datetime import timedelta

class Processing(Component):
    def __init__(self, name, state, date):
        super().__init__(name)
        self.state = state
        self.date = date
        self.schema = [
            'ID', 
            'county', 
            'per_outage_customers_affected', 
            'customers_served',
            'start_time',
            'end_time',
            'duration',
            'emc'
        ]
        self.std = Standardize(name="", state=f"{self.state}", date=f"{self.date}")
        self.col_map = self.std.get_col_map()           # {raw col name : std col name}
        self.col_lists = self.std.get_all_col_lists()   # {std col name : [all raw cols]}
        self.county_map = self.std.get_county_map()     # {raw county name : std county name}
        self.raw_county_list = self.std.get_raw_county_list()        # [county 1, county 2, ...]
        self.master_county_list = self.std.get_master_county_list()  # [county 1, county 2, ...]
        self.county_dfs = {c: pd.DataFrame(columns=self.schema) for c in self.master_county_list}

    # Aggregate data for a given county and provider and add it to the list of dfs to return 
    def aggregate(self, df, county):
        # Get only the columns we need 
        df = df[['county', 'per_outage_customers_affected', 'customers_served', 'timestamp', 'emc']].copy()

        # Setup for outage grouping
        df = df.sort_values('timestamp')
        last_id = self.county_dfs[county]['ID'].max() if not self.county_dfs[county].empty else 0
        threshold = timedelta(minutes=59)

        # Group by timestamp and set IDs for each outage
        df['diff'] = df['timestamp'].diff()             # Get the time difference of curr - previous
        mask = df['diff'] > threshold                   
        df['new_outage'] = (df['diff'].isna() | mask)   # Classify the start of a new outage 
        df['ID'] = df['new_outage'].cumsum() + last_id  # Updates the ID using each new outage to increment ID

        # Compute customers affected deltas 
        df['prev'] = df.groupby('ID')['per_outage_customers_affected'].shift(1).fillna(0)
        df['delta'] = (df['per_outage_customers_affected'] - df['prev']).clip(lower=0)

        # Aggregate result
        result = (
            df.groupby('ID').agg(
                county=('county', 'first'),
                upper=('delta', 'sum'),
                lower=('per_outage_customers_affected', 'max'),
                customers_served=('customers_served', 'max'),
                start_time=('timestamp', 'min'),
                end_time=('timestamp', 'max'),
                emc=('emc', 'first')
            ).reset_index()
        )

        result['middle'] = (result['lower'] + result['upper']) / 2 
        result['duration'] = result['end_time'] - result['start_time']

        if self.county_dfs[county].empty:
            self.county_dfs[county] = result
        else:
            self.county_dfs[county] = pd.concat([self.county_dfs[county], result], ignore_index=True)

    # Creates filler dataframes for counties that had no reported outages for a given day
    def create_filler(self, county):
        print(f"Creating a filler data frame for {county}")
        # Pull historical customers served
        read_path = os.path.join("pipeline\\historicalCustomersServed", f"{self.state}_customers_served.csv")
        historical_val = -1

        # Check that the historical data exists
        try:
            df = pd.read_csv(read_path)
            county_df = df.loc[df['county'] == county]
            
            if not county_df.empty:
                historical_val = int(county_df['customers_served'].sum())
            else:
                print(f"Unable to find historical data for {county}, {self.state}")
        except FileNotFoundError:
            print(f"Unable to find historical.csv for {self.state}")
        
        # Create the filler dataframe using the historical data
        result = pd.DataFrame([{
            "ID": 1,
            "county": county,
            "lower": 0,
            "middle": 0,
            "upper": 0,
            "customers_served": historical_val,
            "start_time": pd.NaT,
            "end_time": pd.NaT,
            "duration": pd.Timedelta(0),
            "emc": ""
        }], columns=self.schema)

        self.county_dfs[county] = result
        # Reindex to reorganize and enforce column order
        self.county_dfs[county] = self.county_dfs[county].reindex(columns=self.schema)

    # Sums each provider's customers served number to be used as the total county customers served
    def aggregate_customers_served(self, county):
        df = self.county_dfs[county]
        df.columns = df.columns.str.strip().str.lower()

        # Enforce numeric datatype
        df['customers_served'] = pd.to_numeric(
            df['customers_served'],
            errors='coerce'
        ).fillna(0).astype(int)

        # Group by EMC and take max per provider then sum each provider
        emc_max = df.groupby('emc', sort=False)['customers_served'].max()
        total = int(emc_max.sum())
        df['customers_served'] = total
        self.county_dfs[county] = df
        

    def process(self, data):
        # Add initial state of processed data to self.county_dfs dictionary
        for i, df in enumerate(data):
            # Standardize data
            df.columns = df.columns.str.strip().str.lower()
            res = self.std.standardize(df)

            # Check that the data is valid
            if res[0]:
                df = res[1]
            else:
                print("The following columns were missing: ")
                print(res[1])
                continue

            # Process each county
            for county in self.master_county_list:
                county_df = df[df['county'] == county]

                if county_df.size != 0:
                    self.aggregate(county_df, county)

        # Create filler dataframes for counties with no reported outages 
        for county in self.master_county_list:
            if self.county_dfs[county].empty:
                self.create_filler(county)

        # Aggregate provider level customers served data into county level data
        for county in self.county_dfs:
            if not self.county_dfs[county].empty:
                self.aggregate_customers_served(county)

        return self.county_dfs

    def run(self, data):
        df_list = data.data[1]
        std_df_list = []

        for df in df_list:
            std_df = self.std.standardize(df.copy())[1]
            std_df_list.append(std_df)

        combined_std_data = pd.concat(std_df_list, ignore_index=True)

        processed_data = self.process([data.data[0].copy()])
        proc_combined = pd.concat(processed_data.values(), ignore_index=True)
        proc_combined = proc_combined.drop(columns=['per_outage_customers_affected'])
        
        output = [combined_std_data, proc_combined]
        print(f"Processing complete for {self.state}")
        
        # once you have data ready for the next step, now we wrap it using the DataWrapper Class
        metadata = {
            "s3_prefix": self.state.lower().strip()
        }

        per_county_data = DataWrapper(data=output, metadata=metadata)
        # we can return this data to the next component of the pipeline
        return per_county_data