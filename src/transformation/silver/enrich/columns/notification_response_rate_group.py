import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class NotificationResponseRateGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "notification_response_rate"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Raramente Responsivos",
            "Ocasionalmente Responsivos",
            "Responsivos",
            "Muito Responsivos",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)