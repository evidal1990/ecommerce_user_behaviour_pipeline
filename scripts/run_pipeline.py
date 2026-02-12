import logging
from pathlib import Path
from src.ingestion.csv_ingestion import CsvIngestion
from src.utils import file_io

BASE_DIR = Path(__file__).resolve().parents[1]


def _load_global_settings() -> dict:
    return file_io.read_yaml(BASE_DIR / "config" / "settings.yaml")


def _setup_logging() -> None:
    log_path = BASE_DIR / "logs" / "pipeline.log"
    log_format = "%(asctime)s | %(levelname)s | %(message)s"
    logging.basicConfig(
        level=logging.INFO, filemode="w", format=log_format, filename=log_path
    )


def execute_pipeline() -> None:
    _setup_logging()
    logging.info("Iniciando o pipeline...")

    try:
        global_settings = _load_global_settings()

        CsvIngestion(global_settings).execute()

    except Exception as exception:
        logging.exception(f"Falha na execução do pipeline {exception}")
        raise

    logging.info("Pipeline finalizada com sucesso.")


execute_pipeline()
