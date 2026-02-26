import polars as pl
from consts.validation_status import ValidationStatus
from src.validation.interfaces.semantic_rule import SemanticRule
from src.utils import dataframe, statistics


class MinValue(SemanticRule):
    def __init__(self, column: str, min_limit: int, sample_size: int = 5) -> None:
        self.column = column
        self.min_limit = min_limit
        self.sample_size = sample_size

    def name(self) -> str:
        return "min_value"

    def validate(self, df: pl.DataFrame) -> dict:
        total_records = df.shape[0]
        users = df.filter(pl.col(self.column) < self.min_limit).select([self.column])
        users_total = len(users)
        if users_total == 0:
            status = ValidationStatus.PASS
            sample = []
            percentage = 0.0
        else:
            status = ValidationStatus.FAIL
            sample = dataframe.get_df_sample(
                df=users, column=self.column, sample_size=self.sample_size
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
