import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class CartAbandonmentRateGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "cart_abandonment_rate"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Raramente Abandonam",
            "Ocasionalmente Abandonam",
            "Frequentemente Abandonam",
            "Muito Frequentemente Abandonam",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
