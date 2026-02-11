import pytest
import polars as pl
from random import random
from src.utils import file_io


def test_read_valid_csv() -> None:
    df = file_io.read_csv("tests/data/test.csv")
    assert df is not None


def test_read_not_found_csv() -> None:
    path = "tests/data/invalid.csv"
    with pytest.raises(FileNotFoundError, match=f"Arquivo não encontrado em {path}"):
        file_io.read_csv(path)


def test_read_valid_yaml() -> None:
    data = file_io.read_yaml("tests/data/test.yaml")
    assert data is not None


def test_read_not_found_yaml() -> None:
    path = "tests/data/invalid.yaml"
    with pytest.raises(FileNotFoundError, match=f"Arquivo não encontrado em {path}"):
        file_io.read_yaml(path)


def test_write_csv() -> None:
    df_path = "tests/data/new.csv"
    df = pl.DataFrame({"col1": random(), "col2": random(), "col3": random()})
    file_io.write_csv(df=df, path=df_path)
    df_read = file_io.read_csv(df_path)
    assert df_read is not None
    assert df_read.equals(df)
