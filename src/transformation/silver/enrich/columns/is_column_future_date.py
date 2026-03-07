import logging
import polars as pl
from datetime import datetime


class IsColumnFutureDate:

    def __init__(self, df: pl.DataFrame) -> None:
        self.df = df

    def _sufix(self) -> str:
        return "is_future"

    def execute(self, columns: list[str]) -> pl.DataFrame:
        for col in columns:
            new_col = f"{col}_{self._sufix()}"
            df = self._create(column=col, new_column=new_col)
            is_true = self._is_true(
                df=df,
                column=new_col,
            )
            is_false = self._is_false(
                df=df,
                column=new_col,
            )
            self._log(
                column=new_col,
                total_records=df.height,
                is_true=is_true,
                is_false=is_false,
            )
        return df

    def _create(
        self,
        column: str,
        new_column: str,
    ) -> pl.DataFrame:
        return self.df.with_columns(
            (pl.col(column) > datetime.now().date()).alias(new_column)
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
        is_true: int,
        is_false: int,
    ) -> None:
        message = (
            f"[CREATE_IS_FUTURE_DATE_COLUMN]\n"
            f"column={column}\n"
            f"total_records={total_records}\n"
            f"{column} → True: {is_true} | False: {is_false}"
        )
        logging.info(message)
