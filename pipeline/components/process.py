from pipeline.base import Component, DataWrapper
from pipeline.components.standardize import Standardize
import os
import pandas as pd
from datetime import timedelta

TIMESTAMP_THRESHOLD = 59    # Measured in minutes

class Processing(Component):
    """
    A component class that receives a list of raw, per provider DataFrames from the retrieve stage,
    standardizes them through the Standardize helper class, groups outages by timestamp, fills in 
    missing / unreported county data, aggregates customers served values across providers, and returns 
    both a combined, standardized DataFrame for SAIDI and a processed, county level DataFrame for SAIFI. 
    """

    def __init__(self, name, state, date):
        """
        Initializes the state being processed and the date we are filtering for. 

        self.schema defines the shape of the DataFrame to be a single, defined form. Every processed
        state DataFrame must conform to this shape.

        self.std is the Standardize helper class instance to help get the various mappings (col_map, 
        col_lists, county_map, raw_county_list, & master_county_list). For more information on 
        mappings, refer to pipeline/mappings/guide.md.

        self.county_dfs is a pre-initialized list of DataFrames for each county in master_county_list.
        This allows process() to safely concatenate counties without needing separate existence validation.
        """
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

    def aggregate(self, df, county):
        """
        For each county, aggregates all outage reports for that county by timestamp. Outages are reported
        as discrete events, typically in 15 minute intervals. When calculating the frequency of outage 
        metric, SAIFI, a single outage may last longer than the reported 15 minute intervals. We define 
        two outages to be part of the same event if their reported timestamps are less than 1 hour apart.
        Outage reports separated by more than 59 minutes are categorized as distinct outage events. 

        Each outage is given a unique ID number within its county, which is initialized to 0 when there 
        are no previous outages or the last ID value used for that county if a previous outage has already
        been recorded. 

        We then need to compute the number of customers affected for each outage. Providers report
        cumulative affected customers per timestamp rather than new customers per event, so the delta
        is used as a best estimate for the total number of customers affected in a given outage event.
        We clip the delta to be 0 to prevent negative deltas from decreasing the counts. The first 
        delta of an outage event is calculated against 0, so the first delta will equal the reported
        customers affected value. 

        Finally, we aggregate the customers affected results into 3 results to feed into SAIFI:
            - Lower:    The single maximum customers affected value. This does not capture scenarios 
                        where in a given outage event, some customers recover their power while others
                        begin losing their power.
            - Upper:    A cumulative sum of all positive deltas that captures when new customers begin 
                        losing power in a single event. This risks "double counting" customers if they
                        recover from an outage but then lose power again within the same event timeframe.
            - Middle:   The mean average between the lower and upper calculations. 

        """

        # Get only the columns we need 
        df = df[['county', 'per_outage_customers_affected', 'customers_served', 'timestamp', 'emc']].copy()

        # Setup for outage grouping: sorting by timestamps, intializing IDs, defining the timestamp threshold 
        df = df.sort_values('timestamp')
        last_id = self.county_dfs[county]['ID'].max() if not self.county_dfs[county].empty else 0
        threshold = timedelta(minutes=TIMESTAMP_THRESHOLD)

        # Group by timestamp and set IDs for each outage
        df['diff'] = df['timestamp'].diff()             # Get the time difference of curr - previous
        mask = df['diff'] > threshold                   # Check if the timestamps are within the same time range
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

        # Calculate the middle customers affected value and duration of each event
        result['middle'] = (result['lower'] + result['upper']) / 2 
        result['duration'] = result['end_time'] - result['start_time']

        # Build / append to the list of county DataFrames
        if self.county_dfs[county].empty:
            self.county_dfs[county] = result
        else:
            self.county_dfs[county] = pd.concat([self.county_dfs[county], result], ignore_index=True)

    def create_filler(self, county):
        """
        When no reports occur for a given county, we still want to provide a DataFrame for that county
        so the downstream metrics stage can produce zero outage reports rather than omitting them completely.
        When a filler DataFrame for a county is needed, a single row for that county is created using 
        0 for all 'affected' columns, and setting the customers served to its most recent value if it exists,
        or to -1 otherwise. 

        TODO: We might be able to remove the historicalCustomersServed logic entirely, using -1 for customers
        served in filler DFs. Then on the metrics / dashboard side of things, we can use the -1 flag to 
        display those counties differently (e.g. gray out counties with no reports for that day).
        """
        
        print(f"Creating a filler data frame for {county}")
        # Pull historical customers served
        read_path = os.path.join("pipeline", "historicalCustomersServed", f"{self.state.upper()}_customers_served.csv")
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
        """
        Calculates the customers served for each county. Since multiple providers can serve the same county,
        a single emc provider's customers served numbers may not represent the entire county. We first 
        group by emc and take each emc's max so that we do not double count emcs. Then, we sum each unique
        emc for a given county to find that county's total customers served value. 
        """

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
        """
        Takes in a combined DataFrames of all providers and processes them into a list of DataFrames
        for each county. There are 3 phases:
            1)  Standardize and aggregate each county
            2)  Fill in counties with no outage reports
            3)  Aggregate customers served values

        Returns:
            self.county_dfs: A dictionary keyed by the standardized county name, whose values represent the processed
            DataFrame for that county.
        """

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
        """
        data is the DataWrapper from the retrieve stage.
            data.data[0]:   The combined DataFrame with all raw data from all providers. This is used to 
                            create the aggregated, processed results that combine outages into events.
                            This is used for calculating the outage frequency index, SAIFI.
            data.data.[1]:  A list of all individual providers. Each provider is standardized on its own,
                            but not fully processed. This keeps each outage report discrete and doesn't 
                            combine them into events. This is more granular and is used to calculate the
                            outage-hours index, SAIDI.

        Returns:
            DataWrapper:        Contains metadata for the state being processed to be passed downstream to
                                uploader. Contains 2 pieces of data, combined_std_data and proc_combined. 
            combined_std_data:  The standardized data for each individual provider. This is the granular
                                data that doesn't combine outages into events, used for SAIDI calculations.
            proc_combined:      The single combined DataFrame that contains outage events rather than discrete
                                reports, aggregated by county and used for SAIFI calculations.
        """

        df_list = data.data[1]  # List of providers and their raw reports
        std_df_list = []

        # Standardize each provider to contain just discrete outages and combine into a list of providers
        for df in df_list:
            std_df = self.std.standardize(df.copy())[1]
            std_df_list.append(std_df)

        # Combine all raw data and process into a list of DataFrames for each county
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
        return per_county_data