import logging
import polars as pl
from consts.action_status import ActionStatus
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class EnrichData:

    def __init__(self, actions: list[EnrichStructure]) -> None:
        self.actions = actions

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        for action in self.actions:
            df = action.execute(df)
            if df.shape[0] == 0:
                status, log_lvl = ActionStatus.FAIL, logging.error
            else:
                status, log_lvl = ActionStatus.PASS, logging.info
            message = (
                f"[ENRICH_DATA_{action.name()}]\n"
                f"status={status}\n"
                f"dataframe={df.schema}\n"
            )
            log_lvl(message)
        return df
