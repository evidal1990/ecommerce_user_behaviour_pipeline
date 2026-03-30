import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class PremiumSubscriptionGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "premium_subscription"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return super().aggregate(df=df, column=self.column)
