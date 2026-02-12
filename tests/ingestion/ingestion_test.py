from pathlib import Path
from datetime import datetime
from src.ingestion.csv_ingestion import CsvIngestion
import polars as pl
import pytest


def test_csv_ingestion() -> None:
    settings = {
        "data": {
            "origin": "tests/data/test.csv",
            "destination": {"raw": f"tests/results/test_{datetime.now()}.csv"},
        }
    }
    CsvIngestion(settings).execute()
    df_origin = pl.read_csv(settings["data"]["origin"])
    df_destination = pl.read_csv(settings["data"]["destination"]["raw"])
    assert df_destination.equals(df_origin)


def test_csv_ingestion_without_settings() -> None:
    with pytest.raises(KeyError, match="data"):
        CsvIngestion({}).execute()


def test_csv_ingestion_without_origin_file() -> None:
    settings = {
        "data": {
            "origin": "",
            "destination": {"raw": f"tests/results/test{datetime.now()}.csv"},
        }
    }
    with pytest.raises(FileNotFoundError, match=f"Arquivo não encontrado em ''"):
        CsvIngestion(settings).execute()


def test_csv_ingestion_without_destiny_file() -> None:
    settings = {
        "data": {
            "origin": "tests/data/test.csv",
            "destination": {"raw": ""},
        }
    }
    with pytest.raises(RuntimeError, match=f"Falha ao escrever CSV em ''"):
        CsvIngestion(settings).execute()
