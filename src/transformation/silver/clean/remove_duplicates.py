import polars as pl

from src.transformation.silver.clean.cleanning_structure import CleanningStructure


class RemoveDuplicates(CleanningStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "REMOVE_DUPLICATED"

    def execute(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_row_index("row_id").unique(subset=["user_id"], keep="first")
