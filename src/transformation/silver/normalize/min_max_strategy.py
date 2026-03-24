import polars as pl

from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class MinMaxScaling(EnrichStructure):
    def __init__(self, column: str) -> None:
        self._column = column

    def name(self) -> str:
        return "MIN_MAX"

    def scaled_column(self) -> str:
        return f"{self._column}_scaled"

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        min = df.select(pl.col(self._column).min()).item()
        max = df.select(pl.col(self._column).max()).item()
        return df.with_columns(
            ((pl.col(self._column) - min) / (max - min)).alias(f"{self._column}_scaled")
        )
