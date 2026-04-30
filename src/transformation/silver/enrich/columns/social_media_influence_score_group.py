import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class SocialMediaInfluenceScoreGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "social_media_influence_score"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Não Influenciados",
            "Influenciados",
            "Altamente Influenciados",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
