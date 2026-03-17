from typing import Any
import polars as pl


class FillNumericColumns:

    def __init__(self) -> None:
        pass

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:

        strategies = {
            pl.Float64: lambda column: (
                pl.when(pl.col(column) < 0)
                .then(None)
                .otherwise(pl.col(column))
                .fill_null(pl.col(column).median())
            ),
            pl.Int64: lambda column: (
                pl.when(pl.col(column) < 0)
                .then(None)
                .otherwise(pl.col(column))
                .fill_null(pl.col(column).median())
            ),
        }
        exprs = map(
            lambda item: strategies.get(
                item[1],
                lambda column: pl.col(
                    column,
                ),
            )(
                item[0]
            ).alias(item[0]),
            df.schema.items(),
        )
        return df.with_columns(list(exprs))
