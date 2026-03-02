from pipeline.base import Component, DataWrapper
from pipeline.components.standardize import Standardize
import pandas as pd


class Metrics(Component):
    def __init__(self, name: str, state, date):
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
        self.df.loc[self.df['county'] == county, col] = val



    def compute_saidi(self, data: pd.DataFrame):
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
        if data is None or len(data) == 0:
            return

        for county in self.county_list:
            cdf = data[data['county'] == county]
            lower, middle, upper = [float('nan')] * 3
            
            if cdf is None or len(cdf) == 0:
                self.set_metric(county, 'lower_saifi', lower)
                self.set_metric(county, 'middle_saifi', middle)
                self.set_metric(county, 'upper_saifi', upper)
                continue

            served = cdf['customers_served'].max()
            if served <= 0 or served is None or served is pd.notna:
                self.set_metric(county, 'lower_saifi', lower)
                self.set_metric(county, 'middle_saifi', middle)
                self.set_metric(county, 'upper_saifi', upper)
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
        if pd.isna(x) or x is None:
            return 0.0
        
        return f"<{threshold}" if abs(x) < threshold else x



    def calculate_metric(self, data) -> pd.DataFrame:
        saidi_data = data[0]
        saifi_data = data[1]

        self.compute_saidi(data=saidi_data)
        self.compute_saifi(data=saifi_data)

        rounded = self.df.copy()
        rounded['saidi'] = rounded['saidi'].round(4)
        rounded['lower_saifi'] = rounded['lower_saifi'].round(4)
        rounded['middle_saifi'] = rounded['middle_saifi'].round(4)
        rounded['upper_saifi'] = rounded['upper_saifi'].round(4)

        self.export = rounded.copy()
        for col in self.metric_cols:
            self.export[col] = self.export[col].apply(self.format_small)
    
        date_str = self.date.isoformat() if self.date else ""
        self.export.insert(5, "date", date_str)
        self.df.insert(5, "date", date_str)

        return [self.df, self.export]
    


    def run(self, data: DataWrapper) -> DataWrapper:
        statewide_df = data.data
        metrics_df = self.calculate_metric(statewide_df)
        metadata = {
            "s3_prefix": self.state.lower().strip()
        }

        return DataWrapper(data=metrics_df, metadata=metadata)