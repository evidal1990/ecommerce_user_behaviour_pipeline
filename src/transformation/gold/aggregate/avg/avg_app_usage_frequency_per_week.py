import polars as pl
from src.transformation.gold.aggregate.median_structure import MedianStructure


class AvgAppUsageFrequencyPerWeek(MedianStructure):
    def __init__(self) -> None:
        pass

    def aggregate(
        self,
        df: pl.DataFrame,
    ) -> pl.Expr:
        return super().aggregate(df=df, column="app_usage_frequency_per_week")
