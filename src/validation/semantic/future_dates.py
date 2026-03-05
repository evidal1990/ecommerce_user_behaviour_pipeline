import polars as pl
from consts.validation_status import ValidationStatus
from src.validation.interfaces.semantic_rule import SemanticRule
from datetime import datetime


class FutureDates(SemanticRule):
    def __init__(
        self, column: str, date_limit: datetime, sample_size: int = 10
    ) -> None:
        self.column = column
        self.date_limit = date_limit
        self.sample_size = sample_size

    def name(self) -> str:
        return "FUTURE_DATES"

    def sample_column(self) -> str:
        return self.column

    def invalid_df(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(pl.col(self.column) > self.date_limit).select([self.column])

    def decide_status(self) -> ValidationStatus:
        condition_has_passed = self._invalid_records == 0
        if self._invalid_percentage < 0.1:
            fail_status = ValidationStatus.WARN
        elif self._invalid_percentage > 0.1 and self._invalid_percentage < 0.5:
            fail_status = ValidationStatus.FAIL
        else:
            fail_status = ValidationStatus.CRITICAL
        return ValidationStatus.PASS if condition_has_passed else fail_status
