import polars as pl


class MeanStructure:
    def __init__(self) -> None:
        pass

    def aggregate(
        self,
        df: pl.DataFrame,
        column: str,
    ) -> pl.Expr:
        return pl.col(column).median().alias(f"avg_{column}")
