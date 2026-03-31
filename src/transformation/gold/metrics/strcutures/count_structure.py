import polars as pl
from src.transformation.gold.metrics.strcutures.base_structure import (
    BaseStructure,
)


class CountStructure(BaseStructure):
    def __init__(
        self,
        metric: str,
        dimension_col: str,
        group_cols: list[str],
    ) -> None:
        super().__init__(
            metric=metric,
            metric_type="count",
            dimension_col=dimension_col,
            group_cols=group_cols,
        )

    def _count_expr(self) -> pl.Expr:
        return pl.count()

    def _aggregate_count(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        return df.group_by(self.all_group_cols).agg(
            self._count_expr().alias("metric_value")
        )

    def _sort_output(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        return df.sort(by=self.all_group_cols)
