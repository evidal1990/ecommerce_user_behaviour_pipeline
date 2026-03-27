import polars as pl
from abc import ABC, abstractmethod


class EnrichStructure(ABC):

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def execute(self, df) -> dict:
        pass

    def aggregate(
        self,
        df: pl.DataFrame,
        column: str,
        labels: list[str] = [],
    ) -> dict:
        if df[column].dtype == pl.Boolean:
            return df.with_columns(
                pl.when(pl.col(column))
                .then(pl.lit("Yes").cast(pl.Utf8))
                .otherwise(pl.lit("No").cast(pl.Utf8))
                .alias(f"{column}_group")
            )

        x_pieces = len(labels)
        group = (
            pl.col(column)
            .qcut(quantiles=x_pieces, labels=labels)
            .alias(f"{column}_group")
        )
        range = pl.col(column).qcut(quantiles=x_pieces).alias(f"{column}_range")
        return df.with_columns([group, range])
