import logging
from pathlib import Path
import polars as pl
from datetime import datetime


class CreateIsFutureDateColumn:

    def __init__(self, df: pl.DataFrame, settings: dict) -> None:
        self.df = df
        self.settings = settings
        self.current_date = datetime.now().date()

    def _sufix(self) -> str:
        return "is_future"

    def execute(self, columns: list[str]) -> pl.DataFrame:
        for col in columns:
            new_col = f"{col}_{self._sufix()}"
            df = self._create(column=col, new_column=new_col)
            self._save(df=df, column=col)
            self._log(
                column=new_col,
                total_records=df.height,
                true_records=self._is_true(
                    df=df,
                    column=new_col,
                ),
                false_records=self._is_false(
                    df=df,
                    column=new_col,
                ),
            )
        return df

    def _create(
        self,
        column: str,
        new_column: str,
    ) -> pl.DataFrame:
        return self.df.with_columns(
            (pl.col(column) > self.current_date).alias(new_column)
        )

    def _is_true(
        self,
        df: pl.DataFrame,
        column: str,
    ) -> int:
        return df.select(pl.col(column).sum()).item()

    def _is_false(
        self,
        df: pl.DataFrame,
        column: str,
    ) -> int:
        return df.select(pl.len() - pl.col(column).sum()).item()

    def _log(
        self,
        column: str,
        total_records: int,
        true_records: int,
        false_records: int,
    ) -> None:
        logging.info(
            (
                f"[CREATE_IS_FUTURE_DATE_COLUMN]\n"
                f"column={column}\n"
                f"total_records={total_records}\n"
                f"{column} → True: {true_records} | False: {false_records}"
            )
        )

    def _save(self, df: pl.DataFrame, column: str) -> None:
        path = f"{self.settings}{column}_future_dates.csv"
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.filter(pl.col(column) > self.current_date).select(
            ["user_id", column]
        ).write_csv(path)
