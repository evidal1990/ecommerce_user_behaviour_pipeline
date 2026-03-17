import polars as pl
import logging
from .format import FormatData
from .remove_duplicates import RemoveDuplicates


class CleanData:
    def __init__(self) -> None:
        pass

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        logging.info(f"Total de registros antes da remoção de duplicatas: {df.height}")
        df = RemoveDuplicates().execute(df=df)
        logging.info(f"Total de registros depois da remoção de duplicatas: {df.height}")
        df = FormatData().execute(df=df)
        return df
