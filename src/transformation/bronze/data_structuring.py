import logging
import polars as pl
from src.transformation.bronze.data_structuring_interface import (
    DataStructuringInterface,
)
from consts.action_status import ActionStatus


class DataStructuring:
    def __init__(
        self, df: pl.DataFrame, fixes: list[DataStructuringInterface]
    ) -> None:
        self.fixes = fixes
        self.df = df

    def execute(self) -> pl.DataFrame:
        for fix in self.fixes:
            self.df = fix.execute(self.df)
            if self.df.shape[0] == 0:
                status, log_lvl = ActionStatus.FAIL, logging.error
            else:
                status, log_lvl = ActionStatus.PASS, logging.info
            message = (
                f"[DATA_STRUCTURING_{fix.name()}]\n"
                f"status={status}\n"
                f"dataframe={self.df.schema}\n"
            )
            log_lvl(message)
        return self.df
