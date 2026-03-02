import pandas as pd
import os
from pprint import pprint
from pipeline.base import Component, DataWrapper
from pipeline.components.standardize import Standardize
from pipeline.components.process import Processing
from pipeline.components.metrics import Metrics
from pipeline.components.uploader import WriteToS3

STATE = "va"
DATE = "2025-05-01"
df = pd.read_csv(f"testing/raw_data/{STATE.lower()}_outages_past_year.csv", low_memory=False)

def test(type):
    std = Standardize(
        name="test",
        state=STATE,
        date=DATE
    )

    proc = Processing(
        name="test",
        state=STATE,
        date=DATE
    )

    met = Metrics(
        name="test",
        state=STATE
    )

    up = WriteToS3(
        name="test",
        
    )

    std_df = std.standardize(df.copy())[1]
    
    proc_res = proc.process([df.copy()])
    proc_df = pd.concat(proc_res.values(), ignore_index=True)
    proc_df = proc_df.drop(columns=['per_outage_customers_affected'])

    dfs = [std_df.copy(), proc_df.copy()]
    met_df = met.calculate_metric(dfs)

    if type == "STD":
        std_df.to_csv(os.path.join("testing", f"{type}_{STATE}_{DATE}.csv"), index=False)
    elif type == "PROC":
        proc_df.to_csv(os.path.join("testing", f"{type}_{STATE}_{DATE}.csv"), index=False)
    elif type == "METRICS":
        met_df[1].to_csv(os.path.join("testing", f"{type}_{STATE}_{DATE}.csv"), index=False)

    print(f"Testing complete for {STATE} {type}")
    
# test("STD")
# test("PROC")
test("METRICS")