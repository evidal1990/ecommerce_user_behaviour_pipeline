import polars as pl
from pathlib import Path
from src.validation.rules.semantic_rule import SemanticRule


BASE_DIR = Path(__file__).resolve().parents[3]


class AllowedColumnValues(SemanticRule):
    def __init__(self, column: str, values: list, sample_size: int = 5) -> None:
        self.column = column
        self.values = values
        self.sample_size = sample_size

    def name(self) -> str:
        return f"ALLOWED_COLUMN_VALUES_{self.column}"

    def sample_column(self) -> str:
        return self.column

    def invalid_df(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(~pl.col(self.column).is_in(self.values))
