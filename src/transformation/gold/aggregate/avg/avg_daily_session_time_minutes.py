import polars as pl

from src.transformation.gold.aggregate.median_structure import MedianStructure


class AvgDailySessionTimeMinutes(MedianStructure):
    def __init__(self) -> None:
        pass

    def aggregate(
        self,
        df: pl.DataFrame,
    ) -> pl.Expr:
        return super().aggregate(df=df, column="daily_session_time_minutes")
