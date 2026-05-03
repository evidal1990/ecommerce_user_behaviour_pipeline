import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class StressFromFinancialDecisionsGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "stress_from_financial_decisions_level"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Despreocupados",
            "Confortáveis",
            "Conscientes",
            "Estressados",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
