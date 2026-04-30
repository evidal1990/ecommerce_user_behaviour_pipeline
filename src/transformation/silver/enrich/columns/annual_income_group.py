import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class AnnualIncomeGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "annual_income"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "E (Classe Baixa)",
            "D (Classe Média Baixa)",
            "C (Classe Média)",
            "B (Classe Média Alta)",
            "A (Classe Alta)",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
