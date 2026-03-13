import pandas as pd
import os
import numpy as np
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
    


def normalize(val):
    if isinstance(val, str) and val.startswith("<"):
        return val  # keep as-is for string comparison
    try:
        return float(val)
    except (ValueError, TypeError):
        return np.nan

def values_match(a, b, tol=0.0001):
    a_norm = normalize(a)
    b_norm = normalize(b)

    # both are the same "<x" string
    if isinstance(a_norm, str) and isinstance(b_norm, str):
        return a_norm == b_norm

    # one is a string threshold, the other is numeric
    if isinstance(a_norm, str) or isinstance(b_norm, str):
        return False

    # both numeric
    if np.isnan(a_norm) and np.isnan(b_norm):
        return True

    return abs(a_norm - b_norm) <= tol

def test_optimization():
    states = ["al", "fl", "ga", "nc"]
    cols_to_check = ["SAIDI", "SAIFI", "LOWER_SAIFI", "UPPER_SAIFI"]

    for state in states:
        baseline = pd.read_csv(f"testing/{state}_baseline.csv")
        optimized = pd.read_csv(f"testing/{state}_optimization.csv")

        merged = baseline.merge(optimized, on="County", suffixes=("_base", "_opt"))
        mismatches = []

        for col in cols_to_check:
            base_col = f"{col}_base"
            opt_col = f"{col}_opt"

            if base_col not in merged.columns or opt_col not in merged.columns:
                continue

            for _, row in merged.iterrows():
                if not values_match(row[base_col], row[opt_col]):
                    mismatches.append({
                        "County": row["County"],
                        "Column": col,
                        "Baseline": row[base_col],
                        "Optimized": row[opt_col]
                    })

        if not mismatches:
            print(f"{state}: MATCH")
        else:
            print(f"{state}: MISMATCH in {len(mismatches)} values")
            print(pd.DataFrame(mismatches).to_string(index=False))


# test("STD")
# test("PROC")
# test("METRICS")
test_optimization()