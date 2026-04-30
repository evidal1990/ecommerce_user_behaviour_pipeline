import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class PurchaseConversionRateGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "purchase_conversion_rate"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Compradores Raros",
            "Compradores Ocasionais",
            "Compradores em Potencial",
            "Compradores Frequentes",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
