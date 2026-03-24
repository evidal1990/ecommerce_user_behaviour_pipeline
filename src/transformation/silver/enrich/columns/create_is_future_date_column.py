from pathlib import Path
import polars as pl
from datetime import datetime

from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class CreateIsFutureDateColumn(EnrichStructure):

    def __init__(
        self,
        settings: dict,
        column: str,
    ) -> None:
        self.column = column
        self.settings = settings
        self.current_date = datetime.now().date()

    def name(self) -> str:
        return f"{self.column.upper()}_IS_FUTURE"

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        new_col = self.name().lower()
        df = self._create(
            df=df,
            column=self.column,
            new_column=new_col,
        )
        self._save_file(
            df=df,
            column=self.column,
        )
        return df

    def _create(
        self,
        df: pl.DataFrame,
        column: str,
        new_column: str,
    ) -> pl.DataFrame:
        return df.with_columns(
            (pl.col(column) > self.current_date).alias(new_column)
        )

    def _save_file(
        self,
        df: pl.DataFrame,
        column: str,
    ) -> None:
        path = f"{self.settings}{column}_future_dates.csv"
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.filter(pl.col(column) > self.current_date).select(
            ["user_id", column]
        ).write_csv(path)
