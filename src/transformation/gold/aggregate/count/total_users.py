import polars as pl

from src.transformation.gold.aggregate.count_structure import CountStructure


class TotalUsers(CountStructure):

    def __init__(self) -> None:
        pass

    def aggregate(
        self,
        df: pl.DataFrame,
    ) -> pl.Expr:
        return super().aggregate(df=df, column="user_id", agg_name="total_users")
