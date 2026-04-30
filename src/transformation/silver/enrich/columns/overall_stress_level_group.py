import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class OverallStressLevelGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "overall_stress_level"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Muito Relaxados",
            "Relaxados",
            "Equilibrados",
            "Estressados",
            "Muito Estressados",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
