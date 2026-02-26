import polars as pl
from consts.validation_status import ValidationStatus
from consts.employment_status import EmploymentStatus
from src.validation.interfaces.semantic_rule import SemanticRule
from src.utils import dataframe, statistics


class EmployedWithoutIncome(SemanticRule):

    def __init__(
        self,
        sample_size: int = 5,
    ) -> None:
        self.column_1 = "employment_status"
        self.column_2 = "annual_income"
        self.sample_size = sample_size

    def name(self) -> str:
        return "employed_user_without_income"

    def validate(self, df: pl.DataFrame) -> dict:
        total_records = df.shape[0]
        condition_1 = pl.col(self.column_1) == EmploymentStatus.EMPLOYED
        condition_2 = pl.col(self.column_2) == 0.0
        users = df.filter(condition_1 & condition_2).select(
            [self.column_2, self.column_1]
        )
        users_total = len(users)
        if users_total == 0:
            status = ValidationStatus.PASS
            sample = []
            percentage = 0.0
        else:
            status = ValidationStatus.WARN
            sample = dataframe.get_df_sample(
                df=users, column=self.column_2, sample_size=self.sample_size
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
