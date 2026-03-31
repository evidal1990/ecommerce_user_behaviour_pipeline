import polars as pl
from src.transformation.gold.metrics.strcutures.percentage_structure import PercentageStructure


class PercentageUsersByCountry(PercentageStructure):
    def __init__(
        self,
        dimension:str,
        group_by: list[str] = [],
    ) -> None:
        super().__init__(
            metric="users_by_country",
            dimension_col=dimension,
            group_cols=group_by,
        )

    def calculate(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        df = self._apply_filter(df)
        df = self._calculate_percentage(df)
        df = self._finalize_output(df)
        return df
