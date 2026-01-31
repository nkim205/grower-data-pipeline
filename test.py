import pandas as pd
import os
from pprint import pprint
from pipeline.base import Component, DataWrapper
from pipeline.components.standardize import Standardize
from pipeline.components.process import Processing


df = pd.read_csv("testing/al_outages_past_year.csv")
# pprint(df)

STATE = "AL"
DATE = "2024-11-21"

# std = Standardize(
#     name="test",
#     state=STATE,
#     date=DATE
# )
# TYPE = "std"
# res = std.standardize(df)
# combined = res[1]

proc = Processing(
    name="test",
    state=STATE,
    date=DATE
)
TYPE = "proc"
res = proc.process([df])
combined = pd.concat(res.values(), ignore_index=True)

combined = combined.drop(columns=['per_outage_customers_affected'])
combined.to_csv(os.path.join("testing", f"{TYPE}_{STATE}_all_counties_{DATE}.csv"), index=False)