from multiprocessing import Value
import polars as pl
import pytest
import re
from datetime import datetime
from ingestion.csv import CsvIngestion


def test_csv_ingestion() -> None:
    """
    Testa se a execução do pipeline de ingestão de CSV com as configurações
    fornecidas gera um arquivo CSV na camada raw com os dados da origem.

    Parâmetros:
        settings (dict): Configurações do pipeline.

    Retorno:
        None
    """
    settings = {
        "data": {
            "ingestion": {
                "origin": "dhrubangtalukdar/e-commerce-shopper-behavior-amazonshopify-based",
                "destination": f"tests/results/test_{datetime.now()}.csv",
            }
        }
    }
    CsvIngestion(settings).execute()
    df_destination = pl.read_csv(settings["data"]["ingestion"]["destination"])
    assert not df_destination.is_empty()


def test_csv_ingestion_without_settings() -> None:
    """
    Testa se a execução do pipeline de ingestão de CSV sem as configurações
    fornecidas gera um erro KeyError com a mensagem "data" não encontrado.

    Parâmetros:
        None

    Retorno:
        None
    """
    with pytest.raises(KeyError, match="data"):
        CsvIngestion({}).execute()


def test_csv_ingestion_without_origin_file() -> None:
    """
    Testa se a execução do pipeline de ingestão de CSV sem o arquivo de origem
    gera um erro FileNotFoundError com a mensagem "Arquivo não encontrado".

    Parâmetros:
        settings (dict): Configurações do pipeline.

    Retorno:
        None
    """
    settings = {
        "data": {
            "ingestion": {
                "origin": "",
                "destination": f"tests/results/test{datetime.now()}.csv",
            }
        }   
    }
    with pytest.raises(ValueError):
        CsvIngestion(settings).execute()


def test_csv_ingestion_without_destiny_file() -> None:
    """
    Testa se a execução do pipeline de ingestão de CSV sem o arquivo de destino
    gera um erro FileNotFoundError com a mensagem "Arquivo não encontrado".

    Parâmetros:
        settings (dict): Configurações do pipeline.

    Retorno:
        None
    """
    settings = {
        "data": {
            "ingestion": {
                "origin": "dhrubangtalukdar/e-commerce-shopper-behavior-amazonshopify-based",
                "destination": "",
            }
        }
    }
    with pytest.raises(FileNotFoundError):
        CsvIngestion(settings).execute()
