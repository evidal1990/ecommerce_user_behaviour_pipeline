import polars as pl


class PercentageStructure:
    def __init__(
        self,
        column: str,
    ) -> None:
        self.column = column

    def calculate(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        return (
            df.group_by(self.column)
            .agg((pl.count() * 100 / df.height).round(2).alias("percentage"))
            .with_columns(
                [
                    pl.lit(self.column).cast(pl.Utf8).alias("dimension"),
                    pl.col(self.column).cast(pl.Utf8).alias("value"),
                ]
            )
            .select(
                [
                    "dimension",
                    "value",
                    "percentage",
                ]
            )
        )
