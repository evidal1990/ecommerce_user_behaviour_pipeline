import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class ReferralCountGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "referral_count"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Raramente Indicam",
            "Ocasionalmente Indicam",
            "Frequentemente Indicam",
            "Defensores da Marca",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)