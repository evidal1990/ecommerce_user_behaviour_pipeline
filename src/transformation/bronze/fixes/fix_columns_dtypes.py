import logging
import polars as pl
from consts.dtypes import DTypes
from src.transformation.bronze.data_structuring_interface import (
    DataStructuringInterface,
)

DTYPES = DTypes.as_dict()


class FixColumnsDTypes(DataStructuringInterface):
    def __init__(self, contract: dict) -> None:
        self._contract = contract

    def name(self) -> str:
        return "fix_columns_dtype"

    def execute(self, df: pl.DataFrame) -> pl.DataFrame:
        for column_name, rules in self._contract["columns"].items():
            if "dtype" in rules:
                dtype = rules["dtype"]
                df = df.with_columns(pl.col(column_name).cast(DTYPES[dtype]))
        return df
