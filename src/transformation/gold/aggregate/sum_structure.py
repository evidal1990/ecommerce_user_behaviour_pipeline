import polars as pl


class SumStructure:
    def __init__(self, column: str, sufix: str) -> None:
        self.column = column
        self.sufix = sufix

    def aggregate(self) -> pl.Expr:
        return pl.col(self.column).sum().alias(f"sum_{self.sufix}")
