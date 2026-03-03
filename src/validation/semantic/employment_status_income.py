import polars as pl
from consts.validation_status import ValidationStatus
from consts.employment_status import EmploymentStatus
from src.validation.interfaces.rule import Rule
from src.utils import dataframe, statistics


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
        total_records = df.shape[0]
        users = self._filter(df)
        users_total = len(users)
        if users_total == 0:
            status = ValidationStatus.PASS
            sample = []
            percentage = 0.0
        else:
            status = ValidationStatus.WARN
            sample = dataframe.get_df_sample(
                df=users, column="annual_income", sample_size=self.sample_size
            )
            percentage = statistics.get_percentage(
                dividend=users_total, divider=total_records
            )
        return {
            "status": status,
            "total_records": total_records,
            "invalid_records": users_total,
            "invalid_percentage": percentage,
            "sample": sample,
        }

    def _filter(self, df) -> pl.DataFrame:
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
