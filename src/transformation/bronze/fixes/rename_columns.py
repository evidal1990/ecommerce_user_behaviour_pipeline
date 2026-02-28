import polars as pl
from src.transformation.bronze.data_structuring_interface import (
    DataStructuringInterface,
)


class RenameColumns(DataStructuringInterface):
    def __init__(self, contract: dict) -> None:
        self._contract = contract

    def name(self) -> str:
        return "rename_columns"

    def execute(self, df: pl.DataFrame) -> pl.DataFrame:
        rename_map = {}
        for column_name, rules in self._contract["columns"].items():
            if "rename" in rules:
                rename_map[column_name] = rules["rename"]
        if rename_map:
            df = df.rename(rename_map)

        return df
