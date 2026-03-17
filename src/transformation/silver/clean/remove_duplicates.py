import polars as pl


class RemoveDuplicates:

    def __init__(self) -> None:
        pass

    def execute(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_row_index("row_id").unique(subset=["user_id"], keep="first")
