import polars as pl
from src.validation.rules.semantic_rule import SemanticRule


class AllowedNullCount(SemanticRule):
    def __init__(self, column: str, sample_size: int = 5) -> None:
        self.column = column
        self.sample_size = sample_size

    def name(self) -> str:
        return f"{self.column}_NOT_NULL_RULE"

    def sample_column(self) -> str:
        return self.column

    def invalid_df(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(pl.col(self.column).null_count() > 0)
