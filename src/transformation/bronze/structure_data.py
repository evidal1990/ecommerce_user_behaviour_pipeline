import logging
import polars as pl
from pathlib import Path
from src.utils import file_io
from src.validation.quality_checks import QualityChecks

BASE_DIR = Path(__file__).resolve().parents[3]


class StructureData:
    def __init__(self, settings) -> None:
        self.df = None
        self.settings = settings["data"]
        self._contract = self._load_contract()

    def execute(self) -> pl.DataFrame:
        self.df = self._read_csv()
        quality_checks = QualityChecks(self.df, self._contract)
        columns = quality_checks._validate_dtypes()
        self._structure_data(columns)
        self._rename_columns()
        
        return self.df

    def _load_contract(self) -> dict:
        contract_path = (
            BASE_DIR / "src" / "transformation" / "bronze" / "schema.yaml"
        )
        return file_io.read_yaml(contract_path)

    def _read_csv(self) -> pl.DataFrame:
        df = pl.read_csv(self.settings["destination"]["raw"])
        if df.is_empty():
            raise ValueError("Dataframe de raw está vazio.")
        logging.info("Leitura de dados na raw concluída com sucesso.")
        return df

    def _structure_data(self, columns: dict) -> None:
        for key, value in columns.items():
            self.df = self.df.with_columns(pl.col(key).cast(value["expected"]))
        logging.info("Dtypes alterados com sucesso.")

    def _rename_columns(self) -> None:
        for key, value in self._contract["from-to"].items():
            self.df = self.df.rename({key: value})
        logging.info(f"Colunas renomeadas com sucesso: {self.df.columns}")
