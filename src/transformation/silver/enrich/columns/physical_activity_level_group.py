import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class PhysicalActivityLevelGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "physical_activity_level"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Atividades Leves",
            "Atividades Moderadas",
            "Atividades Intensas",
            "Atividades Muito Intensas",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
