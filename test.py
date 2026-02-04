import pandas as pd
import os
from pprint import pprint
from pipeline.base import Component, DataWrapper
from pipeline.components.standardize import Standardize
from pipeline.components.process import Processing

STATE = "GA"
DATE = "2025-01-01"
df = pd.read_csv(f"testing/raw_data/{STATE.lower()}_outages_past_year.csv")

def test(type):

    if type == "STD":
        std = Standardize(
            name="test",
            state=STATE,
            date=DATE
        )
        TYPE = "std"
        res = std.standardize(df)
        combined = res[1]
    else:
        proc = Processing(
            name="test",
            state=STATE,
            date=DATE
        )
        TYPE = "proc"
        res = proc.process([df])
        combined = pd.concat(res.values(), ignore_index=True)
        combined = combined.drop(columns=['per_outage_customers_affected'])

    combined.to_csv(os.path.join("testing", f"{TYPE}_{STATE}_{DATE}.csv"), index=False)

# Only run 1 test at a time
# test("STD")
test("PROC")