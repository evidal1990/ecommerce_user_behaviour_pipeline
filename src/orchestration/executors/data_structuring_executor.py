import logging
import polars as pl
from pathlib import Path
from src.utils import file_io
from src.transformation.bronze.data_structuring import DataStructuring
from src.transformation.bronze.fix_columns_dtypes import FixColumnsDTypes
from src.transformation.bronze.rename_columns import RenameColumns

BASE_DIR = Path(__file__).resolve().parents[3]


class DataStructuringExecutor:
    def __init__(self, settings) -> None:
        self._settings = settings

    def execute(self, df: pl.DataFrame) -> None:
        logging.info("Validação semântica do dataframe iniciada")
        self.contract = file_io.read_yaml(
            BASE_DIR / "src" / "validation" / "quality" / "schema.yaml"
        )
        df_with_columns_fixed = DataStructuring(
            FixColumnsDTypes(contract=self.contract)
        ).execute(df)
        df_with_columns_renamed = DataStructuring(
            RenameColumns(contract=self.contract)
        ).execute(df_with_columns_fixed)
        logging.info("Validação semântica do dataframe finalizada\n")
