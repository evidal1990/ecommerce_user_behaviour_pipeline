import logging
import polars as pl
from pathlib import Path
from src.utils import file_io
from src.transformation.bronze.data_structuring import DataStructuring
from src.transformation.bronze.fixes.fix_columns_dtypes import FixColumnsDTypes
from src.transformation.bronze.fixes.rename_columns import RenameColumns

BASE_DIR = Path(__file__).resolve().parents[3]


class DataStructuringExecutor:
    def __init__(self, settings) -> None:
        self._settings = settings

    def execute(self, df: pl.DataFrame) -> None:
        logging.info("Validação semântica do dataframe iniciada")
        self.contract = file_io.read_yaml(
            BASE_DIR / "src" / "validation" / "quality" / "schema.yaml"
        )
        self.df = DataStructuring(
            df,
            [
                FixColumnsDTypes(contract=self.contract),
                RenameColumns(contract=self.contract),
            ],
        ).execute()
        path = self._settings["data"]["bronze"]["destination"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.df.write_csv(path)
        logging.info("Validação semântica do dataframe finalizada\n")
