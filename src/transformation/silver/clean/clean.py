import polars as pl
import logging
from .format import FormatData
from .remove_duplicates import RemoveDuplicates
from .fill_numeric_columns import FillNumericColumns


class CleanData:
    def __init__(self) -> None:
        pass

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        logging.info(f"Total de registros com duplicatas: {df.height}")
        df = RemoveDuplicates().execute(df=df)
        logging.info(f"Total de registros sem duplicatas: {df.height}")

        logging.info("Formatação de strings iniciada")
        df = FormatData().execute(df=df)
        logging.info("Formatação de strings finalizada")

        logging.info("Tratamento de dados numéricos iniciado")
        df = FillNumericColumns().execute(df=df)
        logging.info("Tratamento de dados numéricos finalizado")
        return df
