import polars as pl


class CleanData:
    def __init__(self) -> None:
        pass

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        exprs = [
            self._format(column=col) for col in df.columns if df[col].dtype == pl.String
        ]
        return df.with_columns(exprs)

    def _format(
        self,
        column: str,
    ) -> pl.Expr:
        return (
            pl.col(column)
            .str.strip_chars()
            .str.to_lowercase()
            .str.replace_all(r"\s+", " ")
            .str.to_titlecase()
            .str.replace_all(r"\bA\b", "a")
            .str.replace_all(r"\bE\b", "e")
            .str.replace_all(r"\bI\b", "i")
            .str.replace_all(r"\bO\b", "o")
            .str.replace_all(r"\bU\b", "u")
            .str.replace_all(r"\bOf\b", "of")
            .alias(column)
        )
