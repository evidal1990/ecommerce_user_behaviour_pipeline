import polars as pl
from consts.validation_status import ValidationStatus
from src.validation.interfaces.semantic_rule import SemanticRule


class MinValue(SemanticRule):
    def __init__(self, column: str, min_limit: int, sample_size: int = 5) -> None:
        self.column = column
        self.min_limit = min_limit
        self.sample_size = sample_size

    def name(self) -> str:
        return f"MIN_VALUE_{self.column}"

    def sample_column(self) -> str:
        return self.column

    def invalid_df(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(pl.col(self.column) < self.min_limit).select([self.column])

    def decide_status(self) -> ValidationStatus:
        condition_has_passed = self._invalid_records == 0
        return ValidationStatus.PASS if condition_has_passed else ValidationStatus.FAIL
