import polars as pl
from src.transformation.gold.metrics.strcutures.percentage_over_structure import (
    PercentageOverStructure,
)


class PreferredProductCategory(PercentageOverStructure):
    def __init__(
        self,
        dimension: str,
        group_by: list[str] = [],
    ) -> None:
        super().__init__(
            metric="most_popular_product_category_preference",
            dimension_col=dimension,
            group_cols=group_by,
        )

    def calculate(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        df = self._apply_filter(df)
        result = self.calculate_percentage_over_total(df)
        return self._finalize_output(result)
