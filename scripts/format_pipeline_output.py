#!/usr/bin/env python3
"""
Format pipeline output CSV (county, saidi, middle_saifi, ...) to County, SAIDI, SAIFI, FIPS, State.
Output is named {STATE}_DATA.csv (e.g. GA_DATA.csv, FL_DATA.csv) in the same directory as the input file.

Usage:
  python scripts/format_pipeline_output.py <pipeline_output.csv> [county_fips.csv]

Example:
  python scripts/format_pipeline_output.py testing/fl_pipeline_output_2025-07-31.csv
"""
import argparse
import os
import re
import sys

import pandas as pd


def infer_state_from_filename(path: str) -> str | None:
    """e.g. fl_pipeline_output_2025-07-31.csv -> FL"""
    basename = os.path.basename(path)
    m = re.match(r"^([a-z]{2})_pipeline_output", basename, re.IGNORECASE)
    return m.group(1).upper() if m else None


def coerce_numeric(ser: pd.Series) -> pd.Series:
    """Coerce to float; treat values like '<0.0001' as 0."""
    return pd.to_numeric(ser.astype(str).str.replace("<", "", regex=False), errors="coerce").fillna(0)


def main():
    parser = argparse.ArgumentParser(
        description="Format pipeline output to County, SAIDI, SAIFI, FIPS, State → {STATE}_DATA.csv"
    )
    parser.add_argument("input_csv", help="e.g. testing/fl_pipeline_output_2025-07-31.csv")
    parser.add_argument(
        "fips_mapping",
        nargs="?",
        default=None,
        help="CSV with state_abbr, county, fips, state_name (default: pipeline/mappings/county_fips.csv)",
    )
    args = parser.parse_args()

    state_abbr = infer_state_from_filename(args.input_csv)
    if not state_abbr:
        print("Could not infer state from filename (expect e.g. fl_pipeline_output_2025-07-31.csv).", file=sys.stderr)
        sys.exit(1)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    default_fips = os.path.join(repo_root, "pipeline", "mappings", "county_fips.csv")
    fips_path = args.fips_mapping or default_fips
    if not os.path.isfile(fips_path):
        print(f"FIPS mapping not found: {fips_path}", file=sys.stderr)
        print("Run: python scripts/build_county_fips.py", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.input_csv)
    df = df.dropna(subset=["county"])
    df["county"] = df["county"].astype(str).str.strip()
    df = df[df["county"] != ""]
    df["county_lower"] = df["county"].str.lower()

    mapping = pd.read_csv(fips_path)
    mapping["state_abbr"] = mapping["state_abbr"].astype(str).str.strip().str.upper()
    mapping["county_lower"] = mapping["county"].astype(str).str.strip().str.lower()
    mapping = mapping[mapping["state_abbr"] == state_abbr].drop_duplicates(subset=["county_lower"])

    out = df.merge(
        mapping[["county_lower", "fips", "state_name"]],
        on="county_lower",
        how="left",
    )
    out["County"] = out["county"].str.title()
    out["SAIDI"] = coerce_numeric(out["saidi"])
    out["SAIFI"] = coerce_numeric(out["middle_saifi"])
    out = out[["County", "SAIDI", "SAIFI", "fips", "state_name"]]
    out.columns = ["County", "SAIDI", "SAIFI", "FIPS", "State"]

    input_dir = os.path.dirname(os.path.abspath(args.input_csv))
    out_path = os.path.join(input_dir, f"{state_abbr}_DATA.csv")
    out.to_csv(out_path, index=False)
    print(f"Wrote {len(out)} rows to {out_path}")


if __name__ == "__main__":
    main()
