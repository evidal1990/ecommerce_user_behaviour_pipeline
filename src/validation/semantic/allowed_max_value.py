import polars as pl
from src.validation.rules.semantic_rule import SemanticRule


class AllowedMaxValue(SemanticRule):
    def __init__(self, column: str, max: int, sample_size: int = 5) -> None:
        self.column = column
        self.max_limit = max
        self.sample_size = sample_size

    def name(self) -> str:
        return f"MAX_VALUE_{self.column}"

    def sample_column(self) -> str:
        return self.column

    def invalid_df(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(pl.col(self.column) > self.max_limit).select([self.column])
