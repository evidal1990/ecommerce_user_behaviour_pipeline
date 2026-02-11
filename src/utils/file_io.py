import yaml
import polars as pl
from typing import Any


def read_yaml(path) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return data

    except Exception as excepetion:
        raise FileNotFoundError(f"Arquivo não encontrado em {path}") from excepetion


def read_csv(path: str) -> pl.DataFrame:
    try:
        return pl.read_csv(path)
    except Exception as excepetion:
        raise FileNotFoundError(f"Arquivo não encontrado em {path}") from excepetion


def write_csv(df: pl.DataFrame, path: str) -> None:
    try:
        df.write_csv(path)
    except Exception as exception:
        raise RuntimeError(f"Falha ao escrever CSV em {path}") from exception
