from numpy import divide
import polars as pl
from src.utils import statistics
from src.validation.interfaces.rule import Rule
from consts.validation_status import ValidationStatus


class AllowedMinMaxValues(Rule):
    def __init__(self, column: str, min: int, max: int, sample_size: int = 30) -> None:
        self.column = column
        self.min = min
        self.max = max
        self.sample_size = sample_size

    def name(self) -> str:
        return f"ALLOWED_MIN_MAX_VALUES_{self.column}"

    def validate(self, df: pl.DataFrame) -> dict:
        df_shape = df.shape[0]
        df_filtered = self._filter(df)
        df_filtered_shape = len(df_filtered)

        return {
            "status": (
                ValidationStatus.PASS
                if df_filtered_shape == 0
                else ValidationStatus.FAIL
            ),
            "total_records": df_shape,
            "invalid_records": df_filtered_shape,
            "invalid_percentage": self._get_percentage(
                dividend=df_filtered_shape, divider=df_shape
            ),
            "sample": self._get_sample(df=df_filtered),
        }

    def _filter(self, df: pl.DataFrame) -> pl.DataFrame:
        min, max = df.select(
            pl.col(self.column).min().alias("min_value"),
            pl.col(self.column).max().alias("max_value"),
        ).row(0)

        if min >= self.min and max <= self.max:
            return df.clear()
        return df.filter(
            (pl.col(self.column) < self.min) | (pl.col(self.column) > self.max)
        ).select(
            [
                self.column,
            ]
        )

    def _get_sample(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select(self.column).head(self.sample_size).to_series().to_list()

    def _get_percentage(self, dividend: int, divider: int) -> float:
        return statistics.get_percentage(dividend=dividend, divider=divider)
