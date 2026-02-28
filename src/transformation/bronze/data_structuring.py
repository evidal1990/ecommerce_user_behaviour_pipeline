import logging
import polars as pl
from src.transformation.bronze.data_structuring_interface import (
    DataStructuringInterface,
)
from consts.action_status import ActionStatus


class DataStructuring:
    def __init__(self, fix: DataStructuringInterface) -> None:
        self.fix = fix

    def execute(self, df: pl.DataFrame) -> pl.DataFrame:
        results = {}
        result = self.fix.execute(df)
        results[self.fix.name()] = result
        if len(result) == 0:
            status, log_lvl = ActionStatus.FAIL, logging.error
        else:
            status, log_lvl = ActionStatus.PASS, logging.info
        message = (
            f"[DATA_STRUCTURING_{self.fix.name()}]\n"
            f"status={status}\n"
            f"dataframe={result.schema}\n"
        )
        log_lvl(message)
        return result
