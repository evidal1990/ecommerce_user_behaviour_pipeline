import polars as pl
from src.validation.rules.semantic_rule import SemanticRule


class AllowedMinValue(SemanticRule):
    def __init__(self, column: str, min: int, sample_size: int = 5) -> None:
        self.column = column
        self.min_limit = min
        self.sample_size = sample_size

    def name(self) -> str:
        return f"MIN_VALUE_{self.column}"

    def sample_column(self) -> str:
        return self.column

    def invalid_df(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(pl.col(self.column) < self.min_limit).select([self.column])
