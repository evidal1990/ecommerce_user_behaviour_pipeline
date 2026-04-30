import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class ReviewWrightingFrequencyGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "review_writing_frequency_per_year"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Contribuidores Raros",
            "Contribuidores Ocasionais",
            "Contribuidores Ativos",
            "Contribuidores Poderosos",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
