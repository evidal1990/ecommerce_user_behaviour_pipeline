import polars as pl
import logging
from consts.action_status import ActionStatus
from .cleanning_structure import CleanningStructure


class CleanData:
    def __init__(self, fixes: list[CleanningStructure]) -> None:
        self.fixes = fixes

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        for fix in self.fixes:
            if df.shape[0] == 0:
                status, log_lvl = ActionStatus.FAIL, logging.error
            else:
                status, log_lvl = ActionStatus.PASS, logging.info

            total_missing = (
                df.select(
                    pl.sum_horizontal(pl.all().is_null().cast(pl.Int64))
                    + pl.sum_horizontal(pl.col(pl.FLOAT_DTYPES).is_nan().cast(pl.Int64))
                )
                .sum()
                .item()
            )
            message = (
                f"[DATA_CLEANING_{fix.name()}]\n"
                f"status={status}\n"
                f"rows={df.height}\n"
                f"duplicates={df.is_duplicated().sum()}\n"
                f"total_missing={total_missing}\n"
            )
            log_lvl(message)
        return df
