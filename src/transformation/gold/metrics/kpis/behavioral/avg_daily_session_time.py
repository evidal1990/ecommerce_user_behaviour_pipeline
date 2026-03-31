import polars as pl
from src.transformation.gold.metrics.strcutures.avg_structure import (
    AvgStructure,
)


class AvgDailySessionTime(AvgStructure):
    def __init__(
        self,
        dimension: str,
        group_by: list[str]
    ) -> None:
        super().__init__(
            metric="avg_daily_session_time_in_mninutes",
            column="avg_daily_session_time_minutes",
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
