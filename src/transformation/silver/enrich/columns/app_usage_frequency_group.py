import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class AppUsageFrequencyGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "app_usage_frequency_per_week"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Pouco Ativo",
            "Moderadamente Ativo",
            "Muito Ativo",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
