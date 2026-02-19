import polars as pl


class SemanticRules:
    def __init__(self, df: pl.DataFrame) -> None:
        self.df = df

    def execute(self) -> None:
        pass
