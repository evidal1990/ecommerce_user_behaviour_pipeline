import polars as pl
import logging
from .min_max_strategy import MinMaxStrategy


class Normalize:
    def __init__(self) -> None:
        pass

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        logging.info("Normalização de dados iniciada")
        df = MinMaxStrategy().execute(df=df)
        logging.info("Normalização de dados finalizada")
        return df
