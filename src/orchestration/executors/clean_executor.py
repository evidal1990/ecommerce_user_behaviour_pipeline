import polars as pl
from src.transformation.silver.clean.clean import CleanData
from src.transformation.silver.clean.format import FormatData
from src.transformation.silver.clean.remove_duplicates import RemoveDuplicates
from src.transformation.silver.clean.fill_columns import FillColumns


class CleanExecutor:
    def __init__(self) -> None:
        pass

    def start(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        return CleanData(
            [
                RemoveDuplicates(),
                FormatData(),
                FillColumns(),
            ]
        ).execute(df=df)
