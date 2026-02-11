import polars as pl

DTYPE_MAP = {
    "Int64": pl.Int64(),
    "Float64": pl.Float64(),
    "String": pl.String(),
    "Boolean": pl.Boolean(),
    "Date": pl.Date(),
}


def validate_required_columns(
    df: pl.DataFrame, required_columns: list[str]
) -> list[str]:
    return [col for col in required_columns if col not in df.schema]


def validate_dtypes(df: pl.DataFrame, dtype_schema: dict[str, str]) -> dict:
    result = {}
    for column, dtype_str in dtype_schema.items():
        if dtype_str not in DTYPE_MAP:
            raise Exception(f"Tipo {dtype_str} não está mapeado")

        received = df.schema.get(column)
        if received is None:
            raise Exception(f"Coluna {column} não está mapeada")

        expected = DTYPE_MAP[dtype_str]
        if received != expected:
            result.update({column: {"expected": expected, "received": received}})
    return result
