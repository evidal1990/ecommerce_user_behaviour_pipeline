import polars as pl
from consts.dtypes import DTypes

DTYPES = DTypes.as_dict()


def validate_required_columns(
    df: pl.DataFrame, required_columns: list[str]
) -> list[str]:
    """
    Valida se as colunas obrigatórias est o presentes no DataFrame.

    Parâmetros:
        df (pl.DataFrame): DataFrame a ser validado.
        required_columns (list[str]): Lista de colunas obrigatórias.

    Retorno:
        list[str]: Lista de colunas obrigatórias ausentes no DataFrame.
    """
    return [col for col in required_columns if col not in df.schema]


def validate_dtypes(df: pl.DataFrame, dtype_schema: dict[str, str]) -> dict:
    """
    Valida se os tipos de colunas do DataFrame de origem
    convergem com os tipos especificados no contrato.

    Se houver divergências, registra um log de warning com as colunas
    e seus respectivos tipos divergentes.

    Retorno:
        dict: Dicionário com as colunas e seus respectivos tipos divergentes.

    Exemplo de retorno:
    {
        "col1":
            {
                "expected": pl.Int64(),
                "received": pl.String()
            },
        "col2":
            {
                "expected": pl.Float64(),
                "received": pl.Int64()
            }
    }
    """
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


def get_stats_map(df: pl.DataFrame, columns: list):
    exprs = []
    for col in columns:
        exprs.extend(
            [
                pl.col(col).min().alias(f"{col}__min"),
                pl.col(col).max().alias(f"{col}__max"),
            ]
        )

    stats = df.select(exprs).row(0)
    return dict(zip(df.select(exprs).columns, stats))
