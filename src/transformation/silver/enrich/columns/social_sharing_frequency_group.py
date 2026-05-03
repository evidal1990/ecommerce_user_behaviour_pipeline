import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class SocialSharingFrequencyGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "social_sharing_frequency_per_year"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Raramente",
            "Ocasionalmente",
            "Frequentemente",
            "Sempre",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
