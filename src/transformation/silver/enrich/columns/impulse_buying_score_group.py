import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class ImpulseBuyingScoreGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "impulse_buying_score"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Altamente Ponderados",
            "Compradores Criteriosos",
            "Compradores Equilibrados",
            "Compradores Impulsivos",
            "Compradores Altamente Impulsivos",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
