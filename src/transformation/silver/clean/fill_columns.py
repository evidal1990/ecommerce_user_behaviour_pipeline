import logging
import polars as pl


class FillColumns:

    def __init__(self) -> None:
        pass

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        df_cleaned = self._fill(
            df=df,
        )
        invalid_registries_old = self._invalid_registries(
            df=df,
        )
        invalid_registries_new = self._invalid_registries(
            df=df_cleaned,
        )
        self._log(
            height=df.height,
            invalid_registries_old=invalid_registries_old,
            invalid_registries_new=invalid_registries_new,
        )
        return df_cleaned

    def _invalid_registries(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        pl_int = (pl.col(pl.Int64).is_null()).sum().name.keep()
        pl_float = (
            (pl.col(pl.Float64).is_null() | pl.col(pl.Float64).is_nan())
            .sum()
            .name.keep()
        )
        pl_string = (
            (pl.col(pl.String).is_null() | (pl.col(pl.String).str.strip_chars() == ""))
            .sum()
            .name.keep()
        )
        return (
            df.select(
                [
                    pl_int,
                    pl_float,
                    pl_string,
                ]
            )
            .unpivot(
                variable_name="coluna",
                value_name="invalidos",
            )
            .filter(pl.col("invalidos") > 0)
        )

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

    def _log(
        self,
        height:int,
        invalid_registries_old: pl.DataFrame,
        invalid_registries_new: pl.DataFrame,
    ) -> None:
        message = (
            f"DATA_CLEANING_FILL_EMPTY_REGISTRIES\n"
            f"Registros: {height}\n"
            f"Registros inválidos (antes):{invalid_registries_old}\n"
            f"Registros inválidos (depsois):{invalid_registries_new}:\n"
        )
        logging.info(message)
