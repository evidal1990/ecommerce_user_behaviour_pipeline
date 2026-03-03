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
        contract = self._set_contract()
        df_structured = DataStructuring(
            df,
            [
                FixColumnsDTypes(
                    contract=contract
                ),
                RenameColumns(
                    contract=contract
                ),
            ],
        ).execute()
        self._write_bronze(df_structured)
        logging.info("Validação semântica do dataframe finalizada\n")
        return df_structured

    def _set_contract(self) -> dict:
        path = BASE_DIR.joinpath(
            "src",
            "validation",
            "quality",
            "schema.yaml",
        )
        try:
            return file_io.read_yaml(path)
        except FileNotFoundError:
            logging.error(f"Schema não encontrado em {path}")
            raise

    def _write_bronze(self, df) -> None:
        path = self._settings["data"]["bronze"]["destination"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(path)
