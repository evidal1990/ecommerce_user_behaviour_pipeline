import logging
import polars as pl
from typing import Any
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]


class CleanRawData:
    def __init__(self, settings) -> None:
        self.settings = settings["data"]

    def execute(self) -> Any:
        self.df = self._read_csv()
        self._check_null_count()

    def _read_csv(self) -> pl.DataFrame:
        df = pl.read_csv(self.settings["destination"]["raw"])
        if df.is_empty():
            raise ValueError("Dataframe de raw está vazio.")
        logging.info("Leitura de dados na raw concluída com sucesso.")
        return df

    def _delete_columns(self, columns) -> None:
        if not columns:
            raise ValueError("Nenhuma coluna informada para exclusão.")
        self.df.drop(columns)
        logging.info(f"Colunas excluídas com sucesso: {columns}.")

    def _check_null_count(self) -> None:
        logging.info(f"Verificando total de dados ausentes por coluna...")
        for column in self.df.columns:
            null_count = self.df[column].null_count()
            level = logging.warning if null_count > 0 else logging.info
            level(f"{column}: {null_count}")
        logging.info(f"Verificação de dados ausentes concluída com sucesso.")
