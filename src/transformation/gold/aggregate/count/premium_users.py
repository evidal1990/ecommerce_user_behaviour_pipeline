import polars as pl

from src.transformation.gold.aggregate.count_structure import CountStructure


class PremiumUsers(CountStructure):

    def __init__(self) -> None:
        pass

    def aggregate(
        self,
        df: pl.DataFrame,
    ) -> pl.Expr:
        return super().aggregate(
            df=df, column="premium_subscription", agg_name="premium_users"
        )
