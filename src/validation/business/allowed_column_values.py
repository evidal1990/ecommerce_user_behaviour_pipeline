import polars as pl
from pathlib import Path
from src.utils import statistics
from src.validation.interfaces.rule import Rule
from consts.validation_status import ValidationStatus


BASE_DIR = Path(__file__).resolve().parents[3]


class AllowedColumnValues(Rule):
    def __init__(self, column: str, values: list, sample_size: int = 5) -> None:
        self.column = column
        self.values = values
        self.sample_size = sample_size

    def name(self) -> str:
        return f"ALLOWED_COLUMN_VALUES_{self.column}"

    def validate(self, df: pl.DataFrame) -> dict:
        total_rows =df.height
        df_filtered = self._filter(df=df)
        invalid_rows = df_filtered.height

        status = ValidationStatus.PASS if invalid_rows == 0 else ValidationStatus.FAIL

        percentage = statistics.get_percentage(
            dividend=invalid_rows, divider=total_rows
        )

        sample = self._get_sample(df_filtered)

        return {
            "status": status,
            "total_records": total_rows,
            "invalid_records": invalid_rows,
            "invalid_percentage": percentage,
            "sample": sample,
        }

    def _filter(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(~pl.col(self.column).is_in(self.values))

    def _get_sample(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select(self.column).head(self.sample_size).to_series().to_list()

    def _get_percentage(self, dividend: int, divider: int) -> float:
        return statistics.get_percentage(dividend=dividend, divider=divider)
