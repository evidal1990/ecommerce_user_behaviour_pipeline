import logging
from os import name
import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class SocialMediaInfluenceScoreGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "SOCIAL_MEDIA_INFLUENCE_SCORE_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("social_media_influence_score")
            .map_elements(self._classify)
            .alias("social_media_influence_score_group")
        )

    def _classify(
        self,
        social_media_influence_score: int,
    ) -> str:
        if social_media_influence_score < 0:
            return "Other"
        elif social_media_influence_score <= 4:
            return "Low"
        elif social_media_influence_score <= 7:
            return "Medium"
        return "High"
