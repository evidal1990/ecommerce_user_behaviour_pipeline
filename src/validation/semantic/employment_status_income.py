import polars as pl
from consts.validation_status import ValidationStatus
from consts.employment_status import EmploymentStatus
from src.validation.interfaces.rule import Rule
from src.utils import statistics


class IncomePerEmploymentStatus(Rule):

    def __init__(
        self,
        status: str,
        sample_size: int = 5,
    ) -> None:
        self.status = status
        self.sample_size = sample_size

    def name(self) -> str:
        return f"EMPLOYMENT_STATUS_{self.status}"

    def validate(self, df: pl.DataFrame) -> dict:
        df_shape = df.shape[0]
        df_filtered = self._filter(df)
        df_filtered_shape = len(df_filtered)

        return {
            "status": (
                ValidationStatus.PASS
                if df_filtered_shape == 0
                else ValidationStatus.WARN
            ),
            "total_records": df_shape,
            "invalid_records": df_filtered_shape,
            "invalid_percentage": self._get_percentage(
                dividend=df_filtered_shape, divider=df_shape
            ),
            "sample": self._get_sample(df=df_filtered),
        }

    def _filter(self, df: pl.DataFrame) -> pl.DataFrame:
        condition_1 = pl.col("employment_status").is_in(
            [
                EmploymentStatus.EMPLOYED.value,
                EmploymentStatus.SELF_EMPLOYED.value,
                EmploymentStatus.RETIRED.value,
            ]
        ) & (pl.col("annual_income") > 0.0)

        condition_2 = pl.col("employment_status").is_in(
            [
                EmploymentStatus.STUDENT.value,
                EmploymentStatus.UNEMPLOYED.value,
            ]
        ) & (pl.col("annual_income") == 0.0)

        return df.filter(condition_1 & condition_2).select(
            [
                "annual_income",
                "employment_status",
            ]
        )

    def _get_sample(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select("annual_income").head(self.sample_size).to_series().to_list()

    def _get_percentage(self, dividend: int, divider: int) -> float:
        return statistics.get_percentage(dividend=dividend, divider=divider)
