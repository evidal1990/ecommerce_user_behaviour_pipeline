import polars as pl


class MinMaxStrategy:
    def __init__(self) -> None:
        pass

    def execute(self, df: pl.DataFrame) -> pl.DataFrame:
        return df
