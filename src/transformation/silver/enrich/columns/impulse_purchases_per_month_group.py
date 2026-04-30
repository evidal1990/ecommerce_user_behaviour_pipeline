import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class ImpulsePurchasesPerMonthGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "impulse_purchases_per_month"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Não Impulsivos",
            "Compradores por Impulso Ocasional",
            "Compradores por Impulso Moderado",
            "Compradores por Impulso Frequentemente",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
