import logging
import kagglehub
import polars as pl
from pathlib import Path
from src.ingestion.csv import CsvIngestion
from src.ingestion.ingest import Ingest


class IngestionExecutor:
    def __init__(self, settings: dict) -> None:
        data = settings.get("data")
        if not data or "ingestion" not in data:
            raise ValueError("Configuração de ingestion não encontrada")

        self._settings = data["ingestion"]

    def execute(self) -> pl.DataFrame:
        logging.info("Ingestão de CSV iniciada")

        df = Ingest(
            [CsvIngestion(settings=self._settings, origin=self._ingest_from_kaggle())]
        ).execute()
        logging.info("Ingestão de CSV finalizada\n")
        return df

    def _ingest_from_kaggle(self) -> list:
        if not self._settings["origin"]:
            raise ValueError("Arquivo de origem não informado")

        dataset = Path(kagglehub.dataset_download(self._settings["origin"]))
        dataset_list = list(dataset.glob("*.csv"))
        if dataset_list == []:
            raise FileNotFoundError("Dataset baixado não foi encontrado")
        return dataset_list[0]
