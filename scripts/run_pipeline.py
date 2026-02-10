from pathlib import Path
from src.ingestion import csv_ingestion
from src.utils import file_io

BASE_DIR = Path(__file__).resolve().parents[1]


def _load_global_settings() -> dict:
    return file_io.read_yaml(BASE_DIR / "config" / "settings.yaml")


def execute():
    settings = _load_global_settings()
    csv_ingestion_status = csv_ingestion.execute(settings)
    if csv_ingestion_status:
        print("Ingestão finalizada com sucesso")
    else:
        print("Ingestão finalizada com erro")


execute()
