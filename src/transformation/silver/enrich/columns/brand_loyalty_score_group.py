import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class BrandLoyaltyScoreGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "BRAND_LOYALTY_SCORE_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("brand_loyalty_score")
            .map_elements(self._classify)
            .alias(self.name().lower())
        )

    def _classify(
        self,
        brand_loyalty_score: int,
    ) -> str:
        if brand_loyalty_score < 0:
            return "Other"
        elif brand_loyalty_score <= 6:
            return "Detractors"
        elif brand_loyalty_score <= 8:
            return "Neutral"
        return "Promoters"
