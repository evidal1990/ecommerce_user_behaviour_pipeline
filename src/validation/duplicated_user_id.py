import polars as pl
from consts.error_code import ErrorCode
from consts.validation_status import ValidationStatus
from validation.semantic_rule import SemanticRule
from src.utils import dataframe, statistics


class DuplicatedUserId(SemanticRule):
    def __init__(self, column: str = "user_id", sample_size: int = 10):
        self.column = column
        self.sample_size = sample_size

    def name(self) -> str:
        return "duplicated_user_id"

    def validate(self, df: pl.Dataframe) -> dict:
        total_records = df.shape[0]
        users = (
            self.df["user_id"]
            .filter(self.df["user_id"].is_duplicated())
            .unique()
            .to_list()
        )
        users_total = len(users)
        if users_total > 0:
            status = ValidationStatus.PASS
            sample = []
            percentage = 0.0
        else:
            status = ValidationStatus.FAIL
            sample = dataframe.get_df_sample(
                df, self.column, self.sample_size, users_total
            )
            percentage = statistics.get_percentage(users_total, total_records)
        return {
            "status": status,
            "error_code": ErrorCode.DUPLICATED_USER_ID,
            "total_records": total_records,
            "invalid_records": users_total,
            "invalid_percentage": percentage,
            "sample": sample,
        }
