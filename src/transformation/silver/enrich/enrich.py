import logging
import polars as pl
from consts.action_status import ActionStatus
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class EnrichData:

    def __init__(
        self,
        actions: list[EnrichStructure],
    ) -> None:
        self.actions = actions

    def execute(
        self,
        df,
    ) -> pl.DataFrame:

        for action in self.actions:
            df = action.execute(df)
            if df.shape[0] == 0:
                status = ActionStatus.FAIL
                log_lvl = logging.error
            else:
                status = ActionStatus.PASS
                log_lvl = logging.info
            column = action.name().lower()
            message = (
                f"[ENRICH_DATA_{action.name()}]\n"
                f"status={status}\n"
                f"dataframe={df.select([column]).group_by(column).len()}\n"
            )
            log_lvl(message)
        return df
