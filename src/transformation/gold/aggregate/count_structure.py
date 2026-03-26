import polars as pl


class CountStructure:
    def __init__(self) -> None:
        pass

    def aggregate(
        self,
        column: str,
        df: pl.DataFrame,
        agg_name: str,
    ) -> pl.Expr:
        return pl.col(column).count().alias(agg_name)
