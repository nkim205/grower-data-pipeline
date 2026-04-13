from pipeline.base import Component, DataWrapper
from pipeline.components.standardize import Standardize
import pandas as pd


class Metrics(Component):
    """
    Receives two views of processed data:
        -   A granular standardized view with individual outage reports rather than aggregated outage
            events, used for outage duration calculations, SAIDI
        -   An aggregated, county level view with combined outage events for frequency calculations, SAIFI
    
    Returns both:
        -   A numeric DataFrame for long term metrics and logging
        -   A formatted export friendly version for dashboard displays 
    """

    def __init__(self, name: str, state, date):
        """
        Initializes the state being processed, the date we are looking for, the state's county list, and
        a pre-initialized DataFrame, so every county has an entry in the output even if no outages 
        were reported.
        """
        super().__init__(name)
        self.state = state
        self.date = date
        self.std = Standardize(name="", state=f"{self.state}", date="")
        self.county_list = sorted(self.std.get_master_county_list())
        self.metric_cols = ['saidi', 'lower_saifi', 'middle_saifi', 'upper_saifi']
        self.df = pd.DataFrame({
            "county": self.county_list,
            "saidi": [float('nan')] * len(self.county_list),
            "lower_saifi": [float('nan')] * len(self.county_list),
            "middle_saifi": [float('nan')] * len(self.county_list),
            "upper_saifi": [float('nan')] * len(self.county_list)
        })
        self.export = None
        


    def set_metric(self, county, col, val):
        """
        A helper method that locates the row for a specific county in self.df and sets the specified metrics
        column to the given value. Called upon across each metrics calculation.
        """
        self.df.loc[self.df['county'] == county, col] = val



    def compute_saidi(self, data: pd.DataFrame):
        """
        For each county in the granular discrete outage report data, calculates its SAIDI value as:
            (sum of customers affected per outage * duration of each outage) / customers served
        """
        if data is None or len(data) == 0: 
            return

        for county in self.county_list:
            cdf = data[data['county'] == county]
            saidi = float('nan')
            
            if cdf is None or len(cdf) == 0:
                self.set_metric(county, 'saidi', saidi)
                continue

            served = cdf['customers_served'].max()
            sum_affected = cdf['per_outage_customers_affected'].sum()
            affected_hours = sum_affected * 0.25 # each interval is 1/4 of an hour
            saidi = affected_hours / served
            self.set_metric(county, 'saidi', saidi)



    def compute_saifi(self, data: pd.DataFrame):
        """
        For each county in the aggregated outage events data, calculates the SAIFI value as:
            total customers affected / customers served
        
        Each metric uses the following from the aggregated outage data as its 'customers affected' value:
            lower_saifi: lower
            middle_saifi: middle
            upper_saifi: upper
        
        In cases where there were no reports for a given county (i.e. customers served <= 0 or the
        county DataFrame is empty), we set all metrics values to equal NaN. This is used by the 
        dashboard to differentiate reports that were "rounded" to 0 vs. no reports at all.
        """

        if data is None or len(data) == 0:
            return

        for county in self.county_list:
            cdf = data[data['county'] == county]
            lower, middle, upper = [float('nan')] * 3
            skip = False
            
            if cdf is None or len(cdf) == 0:
                self.set_metric(county, 'lower_saifi', lower)
                self.set_metric(county, 'middle_saifi', middle)
                self.set_metric(county, 'upper_saifi', upper)
                skip = True

            served = cdf['customers_served'].max()
            if served <= 0 or served is None or pd.isna(served):
                self.set_metric(county, 'lower_saifi', lower)
                self.set_metric(county, 'middle_saifi', middle)
                self.set_metric(county, 'upper_saifi', upper)
                skip = True
            
            if skip:
                continue

            lower_sum = cdf['lower'].sum() 
            lower = lower_sum / served
            middle_sum = cdf['middle'].sum()
            middle = middle_sum / served
            upper_sum = cdf['upper'].sum()
            upper = upper_sum / served

            self.set_metric(county, 'lower_saifi', lower)
            self.set_metric(county, 'middle_saifi', middle)
            self.set_metric(county, 'upper_saifi', upper)



    def format_small(self, x, threshold=0.0001):
        """
        This helper method formats the metrics values to be more readable, returning a formatted
        string of '<{threshold}' when the metric value is too small, the metric value itself when it
        is valid, or 0.0 for when a county had no outage reports. 

        This prevents counties with very little reports from having metrics being rounded to 0, which 
        would be indistinguishable from counties that had no reports at all.
        """
        if pd.isna(x) or x is None:
            return 0.0
        
        return f"<{threshold}" if abs(x) < threshold else x



    def calculate_metric(self, data) -> pd.DataFrame:
        """
        Receives data, a 2 element list where:
            data[0]: The granular, standardized DataFrame with individual outages used for SAIDI
            data[1]: The aggregated, processed DataFrame with outage events used for SAIFI

        Returns:
            self.df:    The purely numeric value metrics rounded to 4 decimal places to be used for 
                        historical metrics storage and calculation.
            self.export:    The small formatted metrics that has string formatted values to be used
                            for the dashboard display.
        """
        
        # Initialize data to be used for each metrics
        saidi_data = data[0]
        saifi_data = data[1]

        self.compute_saidi(data=saidi_data)
        self.compute_saifi(data=saifi_data)

        # Round values to 4 decimal places
        rounded = self.df.copy()
        rounded['saidi'] = rounded['saidi'].round(4)
        rounded['lower_saifi'] = rounded['lower_saifi'].round(4)
        rounded['middle_saifi'] = rounded['middle_saifi'].round(4)
        rounded['upper_saifi'] = rounded['upper_saifi'].round(4)

        # Create the export friendly version
        self.export = rounded.copy()
        for col in self.metric_cols:
            self.export[col] = self.export[col].apply(self.format_small)
    
        # Add date column to each DF
        date_str = self.date.isoformat() if self.date else ""
        self.export.insert(5, "date", date_str)
        self.df.insert(5, "date", date_str)

        return [self.df, self.export]
    


    def run(self, data: DataWrapper) -> DataWrapper:
        """
        data.data is the [combined_std_data, proc_combined] produced from process.py

        Returns a DataWrapper with the state as its metadata and [self.df, self.export] as the data payload.
        """
        statewide_df = data.data
        metrics_df = self.calculate_metric(statewide_df)
        metadata = {
            "s3_prefix": self.state.lower().strip()
        }

        return DataWrapper(data=metrics_df, metadata=metadata)