import polars as pl
from src.utils import dataframe, statistics
from src.validation.interfaces.rule import Rule
from consts.validation_status import ValidationStatus


class NotAllowedNullCount(Rule):
    def __init__(self, column: str, sample_size: int = 5) -> None:
        self.column = column
        self.sample_size = sample_size

    def name(self) -> str:
        return f"not_allowed_null_count_{self.column}"

    def validate(self, df: pl.DataFrame) -> dict:
        total_records = df.shape[0]
        users = df.filter(pl.col(self.column).null_count() > 0)
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
