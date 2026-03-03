import polars as pl
from consts.validation_status import ValidationStatus
from src.validation.interfaces.rule import Rule
from src.utils import dataframe, statistics


class DuplicatedUserId(Rule):
    def __init__(self, sample_size: int = 10) -> None:
        self.sample_size = sample_size

    def name(self) -> str:
        return "DUPLICATED_USER_ID"

    def validate(self, df: pl.DataFrame) -> dict:
        total_records = df.shape[0]
        users = (
            df["user_id"].filter(df["user_id"].is_duplicated()).unique().to_list()
        )
        users_total = len(users)
        if users_total == 0:
            status = ValidationStatus.PASS
            sample = []
            percentage = 0.0
        else:
            status = ValidationStatus.FAIL
            sample = dataframe.get_df_sample(
                df=users, column="user_id", sample_size=self.sample_size
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
