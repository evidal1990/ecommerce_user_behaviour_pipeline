from argparse import FileType
import yaml
import polars as pl


def read_yaml(path):
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")


def read_csv(path):
    try:
        return pl.read_csv(path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")


def write_csv(df: pl.DataFrame, path: str):
    try:
        df.write_csv(path)
    except FileNotFoundError:
        raise Exception(f"Erro ao tentar gravar o arquivo {path}")
