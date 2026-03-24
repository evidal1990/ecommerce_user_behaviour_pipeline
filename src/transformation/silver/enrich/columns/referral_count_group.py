import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class ReferralCountGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "REFERRAL_COUNT_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("referral_count")
            .map_elements(self._classify)
            .alias(self.name().lower())
        )

    def _classify(
        self,
        referral_count: int,
    ) -> str:
        if referral_count < 0:
            return "Other"
        elif referral_count == 0:
            return "Non-referrer"
        elif referral_count <= 2:
            return "Low"
        elif referral_count <= 5:
            return "Moderate"
        return "High"
