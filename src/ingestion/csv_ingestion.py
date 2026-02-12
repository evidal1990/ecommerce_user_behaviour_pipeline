import logging
import polars as pl
from pathlib import Path
from src.utils import file_io, dataframe

BASE_DIR = Path(__file__).resolve().parents[2]


class CsvIngestion:
    def __init__(self, settings: dict) -> None:
        self.df = None
        self._settings = settings["data"]
        self._contract = self._load_contract()

    def execute(self) -> None:
        self.df = self._read_csv()
        self._validate_required_columns()
        self._validate_dtypes()
        self._write_raw()

    def _read_csv(self) -> pl.DataFrame:
        df = pl.read_csv(self._settings["origin"])
        if df.is_empty():
            raise ValueError("Dataframe de origem está vazio.")
        logging.info("Leitura de dados na origem concluída com sucesso.")
        return df

    def _validate_required_columns(self) -> None:
        missing_columns = dataframe.validate_required_columns(
            df=self.df, required_columns=self._contract["required_columns"]
        )
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")
        logging.info("Validação de colunas obrigatórias concluída com sucesso")

    def _validate_dtypes(self) -> None:
        divergences = dataframe.validate_dtypes(
            df=self.df, dtype_schema=self._contract["dtypes"]
        )
        if divergences:
            logging.warning(f"Colunas com tipos divergentes: {divergences}")
        logging.info("Validação de tipos de colunas concluída com sucesso.")

    def _write_raw(self) -> None:
        self.df.write_csv(self._settings["destination"]["raw"])
        logging.info("Escrita de dados na camada raw concluída com sucesso")

    def _load_contract(self) -> dict:
        contract_path = BASE_DIR / "src" / "ingestion" / "schema.yaml"
        return file_io.read_yaml(contract_path)
