import polars as pl
from src.transformation.gold.metrics.strcutures.base_structure import (
    BaseStructure,
)


class AvgStructure(BaseStructure):
    def __init__(
        self,
        metric: str,
        column: str,
        dimension_col: str,
        group_cols: list[str] = [],
    ) -> None:
        self.column = column
        super().__init__(
            metric=metric,
            metric_type="average",
            dimension_col=dimension_col,
            group_cols=group_cols,
        )

    def _calculate_average(
        self,
        df: pl.DataFrame,
        column: str,
    ) -> pl.DataFrame:
        return self._aggregate_weighted_avg(df, column)

    def _weighted_avg_expr(
        self,
        column: str,
    ) -> pl.Expr:
        return (
            pl.col(column)
            .mul(pl.col("count_users"))
            .sum()
            .truediv(pl.col("count_users").sum())
            .round(2)
        )

    def _aggregate_weighted_avg(
        self,
        df: pl.DataFrame,
        column: str,
    ) -> pl.DataFrame:
        return df.group_by(self.all_group_cols).agg(
            self._weighted_avg_expr(column).alias("metric_value")
        )
