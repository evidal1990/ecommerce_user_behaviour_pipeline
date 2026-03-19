import polars as pl
from src.transformation.silver.clean.cleanning_structure import CleanningStructure


class FillColumns(CleanningStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "FILL_MISSING_VALUES"

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        df_cleaned = self._fill(
            df=df,
        )
        return df_cleaned

    def _fill(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        strategies = {
            pl.Float64: lambda col: self._fill_numeric(
                df=df,
                col=col,
            ),
            pl.Int64: lambda col: self._fill_numeric(
                df=df,
                col=col,
            ),
            pl.String: lambda col: self._fill_literal(
                col=col,
            ),
        }
        exprs = map(
            lambda item: strategies.get(
                item[1],
                lambda column: pl.col(
                    column,
                ),
            )(
                item[0]
            ).alias(item[0]),
            df.schema.items(),
        )
        return df.with_columns(list(exprs))

    def _fill_numeric(
        self,
        df: pl.DataFrame,
        col: str,
    ) -> pl.Expr:
        median = df.select(
            pl.col(col).fill_nan(None).filter(pl.col(col) >= 0).median()
        ).item()
        return (
            pl.when(pl.col(col) < 0)
            .then(None)
            .otherwise(pl.col(col))
            .fill_nan(None)
            .fill_null(median)
        )

    def _fill_literal(
        self,
        col: str,
    ) -> pl.Expr:
        return (
            pl.col(col)
            .str.strip_chars()
            .pipe(lambda col: col.replace("", None))
            .fill_null("Missing")
        )
