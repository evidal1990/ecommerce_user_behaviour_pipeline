import polars as pl
from consts.validation_status import ValidationStatus
from src.validation.semantic_rule import SemanticRule
from src.utils import dataframe, statistics


class DuplicatedUserId(SemanticRule):
    def __init__(self, sample_size: int = 10) -> None:
        self.column = "user_id"
        self.sample_size = sample_size

    def name(self) -> str:
        return "duplicated_user_id"

    def validate(self, df: pl.DataFrame) -> dict:
        total_records = df.shape[0]
        users = (
            df[self.column]
            .filter(df[self.column].is_duplicated())
            .unique()
            .to_list()
        )
        users_total = len(users)
        if users_total == 0:
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
            "total_records": total_records,
            "invalid_records": users_total,
            "invalid_percentage": percentage,
            "sample": sample,
        }
