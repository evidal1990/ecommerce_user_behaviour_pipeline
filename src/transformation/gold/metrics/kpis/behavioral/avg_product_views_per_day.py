import polars as pl
from src.transformation.gold.metrics.strcutures.avg_structure import (
    AvgStructure,
)


class AvgProductViewsPerDay(AvgStructure):
    def __init__(
        self,
        dimension: str,
        group_by: list[str] = [],
    ) -> None:
        super().__init__(
            metric="avg_product_views_per_day",
            column="avg_product_views_per_day",
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
