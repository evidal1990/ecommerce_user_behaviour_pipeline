from abc import abstractmethod
import polars as pl


class BaseStructure:
    def __init__(
        self,
        metric: str,
        metric_type: str,
        dimension_col: str,
        group_cols: list[str] | None = None,
    ) -> None:
        self.metric = metric
        self.metric_type = metric_type
        self.dimension_col = dimension_col
        self.group_cols = group_cols or []

    @property
    def all_group_cols(self) -> list[str]:
        return [self.dimension_col] + self.group_cols

    @abstractmethod
    def calculate(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        pass

    def _apply_filter(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        return df

    def _finalize_output(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        return df.with_columns(
            [
                pl.lit(self.metric).alias("metric"),
                pl.lit(self.metric_type).alias("metric_type"),
                pl.lit(self.dimension_col).alias("dimension"),
                pl.col(self.dimension_col).cast(pl.Utf8).alias("value"),
            ]
        ).select(
            [
                "metric",
                "metric_type",
                "dimension",
                "value",
                *self.group_cols,
                "metric_value",
            ]
        )
