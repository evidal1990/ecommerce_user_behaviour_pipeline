import logging
import polars as pl
from pathlib import Path
from src.utils import file_io
from src.transformation.silver.enrich.columns.create_is_future_date_column import (
    CreateIsFutureDateColumn,
)


BASE_DIR = Path(__file__).resolve().parents[4]


class EnrichDataFrame:

    def __init__(self) -> None:
        self.contract = self._load_contract()["actions"]["enrich"]

    def execute(self, df) -> pl.DataFrame:
        df_after_create_is_future_date_columns = self._is_future_date(df=df)
        return df_after_create_is_future_date_columns

    def _is_future_date(self, df) -> pl.DataFrame:
        columns = self.contract["is_future_date"]["columns"]
        df = CreateIsFutureDateColumn(df=df).execute(columns=columns)
        return df

    def _load_contract(self) -> dict:
        path = BASE_DIR.joinpath(
            "src",
            "transformation",
            "silver",
            "schema.yaml",
        )
        try:
            return file_io.read_yaml(path)
        except FileNotFoundError:
            logging.error(f"Schema não encontrado em {path}")
            raise
