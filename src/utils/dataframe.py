import polars as pl
from typing import Any


def get_df_sample(df: pl.DataFrame, column: str, sample_size: int) -> list[Any]:
    return df.select(column).head(sample_size).to_series().to_list()
