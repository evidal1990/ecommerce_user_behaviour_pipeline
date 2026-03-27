import polars as pl


class CountStructure:
    def __init__(self, column: str, sufix: str) -> None:
        self.column = column
        self.sufix = sufix

    def aggregate(self) -> pl.Expr:
        return pl.col(self.column).count().alias(f"count_{self.sufix}")
