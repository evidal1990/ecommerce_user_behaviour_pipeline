import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class ReturnRateGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "RETURN_RATE_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("return_rate")
            .map_elements(self._classify)
            .alias(self.name().lower())
        )

    def _classify(
        self,
        return_rate: int,
    ) -> str:
        if return_rate < 0:
            return "Other"
        elif return_rate <= 20:
            return "Very Low"
        elif return_rate <= 40:
            return "Low"
        elif return_rate <= 60:
            return "Moderate"
        elif return_rate <= 80:
            return "High"
        return "Very High"
