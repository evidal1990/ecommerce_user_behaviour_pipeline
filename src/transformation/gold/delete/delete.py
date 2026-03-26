import polars as pl
import logging
from consts.action_status import ActionStatus


class Delete:
    def __init__(
        self,
        columns: list[str],
    ) -> None:
        self.columns = columns

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        df_cleaned = df.drop(self.columns)
        if df_cleaned.shape[0] == 0:
            status = ActionStatus.FAIL
            log_lvl = logging.error
        else:
            status = ActionStatus.PASS
            log_lvl = logging.info
        message = (
            f"[DELETE_UNUSED_COLUMNS]\n"
            f"status={status}\n"
            f"deleted_columns={self.columns}\n"
            f"dataframe={df_cleaned.schema}\n"
        )
        log_lvl(message)
        return df_cleaned
