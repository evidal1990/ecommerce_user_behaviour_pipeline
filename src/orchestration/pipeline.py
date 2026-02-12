import logging
from src.ingestion.csv_ingestion import CsvIngestion


class Pipeline:
    def __init__(self, settings: dict) -> None:
        self.settings = settings

    def run(self) -> None:
        logging.info("Ingestão de CSV iniciada...")
        CsvIngestion(self.settings).execute()
        logging.info("Ingestão de CSV finaliza...")
