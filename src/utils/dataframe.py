import polars as pl
from consts.dtypes import DTypes

DTYPES = DTypes.as_dict()


def validate_required_columns(
    df: pl.DataFrame, required_columns: list[str]
) -> list[str]:
    return [col for col in required_columns if col not in df.schema]


def validate_dtypes(df: pl.DataFrame, dtype_schema: dict[str, str]) -> dict:
    result = {}
    for column, dtype_str in dtype_schema.items():
        if dtype_str not in DTYPES:
            raise TypeError(f"Tipo {dtype_str} não está mapeado")

        received = df.schema.get(column)
        if received is None:
            raise ValueError(f"Coluna {column} não está mapeada no schema")

        expected = DTYPES[dtype_str]
        if received != expected:
            result.update({column: {"expected": expected, "received": received}})
    return result
