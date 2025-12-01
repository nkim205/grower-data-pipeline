from pipeline.base import Component, DataWrapper
import pandas as pd


class Metrics(Component):
    def __init__(self, name: str):
        super().__init__(name)

    def compute_saidi(self, data: pd.DataFrame, affected_col: str) -> float:
        if data is None or data.empty:
            return 0.0

        customers_served = pd.to_numeric(
            data["customers_served"], errors="coerce"
        ).dropna()

        if customers_served.empty:
            return 0.0

        served = customers_served.max()
        if served <= 0:
            return 0.0

        duration_hours = (
            pd.to_timedelta(data["duration"], errors="coerce")
            .dt.total_seconds()
            .fillna(0)
            / 3600.0
        )

        affected = pd.to_numeric(
            data[affected_col], errors="coerce"
        ).fillna(0)

        saidi_hours = (duration_hours * affected).sum() / served
        return float(saidi_hours * 60.0)

    def compute_saifi(self, data: pd.DataFrame, affected_col: str) -> float:
        if data is None or data.empty:
            return 0.0

        customers_served = pd.to_numeric(
            data["customers_served"], errors="coerce"
        ).dropna()

        if customers_served.empty:
            return 0.0

        served = customers_served.max()
        if served <= 0:
            return 0.0

        affected = pd.to_numeric(
            data[affected_col], errors="coerce"
        ).fillna(0)

        return float(affected.sum() / served)

    def calculate_metric(self, data: pd.DataFrame) -> pd.DataFrame:
        if data is None or data.empty:
            return pd.DataFrame(
                columns=[
                    "county_name",
                    "lower_saidi",
                    "upper_saidi",
                    "lower_saifi",
                    "upper_saifi",
                ]
            )

        rows = []

        for county, df_county in data.groupby("county"):
            lower_saidi = self.compute_saidi(
                df_county, "daily_max_customers_affected"
            )
            upper_saidi = self.compute_saidi(
                df_county, "per_outage_customers_afffected"
            )

            lower_saifi = self.compute_saifi(
                df_county, "daily_max_customers_affected"
            )
            upper_saifi = self.compute_saifi(
                df_county, "per_outage_customers_afffected"
            )

            rows.append(
                {
                    "county_name": county,
                    "lower_saidi": lower_saidi,
                    "upper_saidi": upper_saidi,
                    "lower_saifi": lower_saifi,
                    "upper_saifi": upper_saifi,
                }
            )

        return pd.DataFrame(rows)

    def run(self, data: DataWrapper) -> DataWrapper:
        statewide_df = data.data  
        metrics_df = self.calculate_metric(statewide_df)
        return DataWrapper(metrics_df)

