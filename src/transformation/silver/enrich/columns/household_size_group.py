import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class HouseholdSizeGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "household_size"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Família Muito Pequena",
            "Família Pequena",
            "Família Média",
            "Família Grande",
            "Família Muito Grande",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
