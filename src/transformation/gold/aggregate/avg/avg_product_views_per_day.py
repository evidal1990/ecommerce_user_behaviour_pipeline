import polars as pl
from src.transformation.gold.aggregate.median_structure import MedianStructure


class AvgProductViewsPerDay(MedianStructure):
    def __init__(self) -> None:
        pass

    def aggregate(
        self,
        df: pl.DataFrame,
    ) -> pl.Expr:
        return super().aggregate(df=df, column="product_views_per_day")
