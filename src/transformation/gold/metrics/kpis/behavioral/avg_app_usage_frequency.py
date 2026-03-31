import polars as pl
from src.transformation.gold.metrics.strcutures.avg_structure import (
    AvgStructure,
)


class AvgAppUsageFrequency(AvgStructure):
    def __init__(
        self,
        dimension: str,
        group_by: list[str] = [],
    ) -> None:
        super().__init__(
            metric="avg_app_usage_frequency_per_week_in_days",
            column="avg_app_usage_frequency_per_week",
            dimension_col=dimension,
            group_cols=group_by,
        )

    def calculate(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        df = self._apply_filter(df)
        df = self._calculate_average(
            df,
            column=self.column,
        )
        df = self._finalize_output(df)
        return df
