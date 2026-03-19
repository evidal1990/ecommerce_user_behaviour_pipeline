import polars as pl
import logging
from .format import FormatData
from .remove_duplicates import RemoveDuplicates
from .fill_columns import FillColumns


class CleanData:
    def __init__(self) -> None:
        pass

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        logging.info("Limpeza de dados iniciada")
        df = RemoveDuplicates().execute(df=df)
        df = FormatData().execute(df=df)
        df = FillColumns().execute(df=df)
        logging.info("Limpeza de dados finalizada")
        return df
