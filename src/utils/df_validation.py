import polars as pl
import warnings
from pathlib import Path
from src.utils import file_io

TYPE_MAP = {
    "Int64": pl.Int64(),
    "Float64": pl.Float64(),
    "String": pl.String(),
    "Boolean": pl.Boolean(),
    "Date": pl.Date(),
}


def load_yaml(parent_level: int, folder: str, file: str) -> dict:
    BASE_DIR = Path(__file__).resolve().parents[parent_level]
    return file_io.read_yaml(BASE_DIR / folder / f"{file}.yaml")


def validate_required_columns(df: pl.DataFrame, required_columns: list[str]):
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(
            f"Colunas obrigatórias ausentes: {missing}"
        )


def validate_required_columns(df: pl.DataFrame, required_columns: list[str]):
    missing = [col for col in required_columns if col not in df.schema]
    if missing:
        raise ValueError(
            f"Colunas obrigatórias ausentes no CSV: {missing}"
        )


def validate_dtypes(df: pl.DataFrame, dtype_schema: dict[str, str]):
    for column, dtype_str in dtype_schema.items():
        if dtype_str not in TYPE_MAP:
            raise ValueError(
                f"Tipo {dtype_str} não suportado no schema"
            )

        received = df.schema.get(column)
        if received is None:
            raise ValueError(
                f"Coluna {column} não encontrada para validação de tipo"
            )

        expected = TYPE_MAP[dtype_str]

        if received != expected:
            warnings.warn(
                f"Coluna {column}: esperado {expected}, recebido {received}",
                category=UserWarning,
            )
