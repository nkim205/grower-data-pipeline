from pipeline.base import Component, DataWrapper
import pandas as pd

class Metrics(Component):
    def __init__(self, name):
        super().__init__(name)
        
    def compute_saidi(self, data):
        if data is None:
            return 0.0
        customers_served = pd.to_numeric(data["customers_served"],errors="coerce").dropna()
        if customers_served.empty:
            return 0.0
        served = customers_served.max()
        if served <= 0:
            return 0.0
        duration = pd.to_timedelta(data["duration"], errors="coerce").dt.total_seconds().fillna(0) / 3600
        affected = pd.to_numeric(data["per_outage_customers_affected"], errors="coerce").fillna(0)

        saidi_hours = (duration * affected).sum() / served
        return saidi_hours * 60 # minutes per customer
    
    def compute_saifi(self, data):
        if data is None:
            return 0.0
        customers_served = pd.to_numeric(data["customers_served"], errors="coerce").dropna()
        if customers_served.empty:
            return 0.0
        served = customers_served.max()
        if served <= 0:
            return 0.0

        affected = pd.to_numeric(data["per_outage_customers_affected"],errors="coerce").fillna(0)

        return affected.sum() / served




    def calculate_metric(self, data):
        rows = []
        for county, dataframe in data.items():
            saidi = self.compute_saidi(dataframe)
            saifi = self.compute_saifi(dataframe)
            rows.append({"county" : county, "saidi": saidi, "saifi": saifi})

        return pd.DataFrame(rows)
    
    def run(self, data):
        # this is the main function to be implemented, use your helper functions here 
        processed_data = data.data
        saidi = self.calculate_metric(processed_data)
        # once you have data ready for the next step, now we wrap it using the DataWrapper Class
        saidi_per_county = DataWrapper(saidi)
        # we can return this data to the next component of the pipeline
        return saidi_per_county