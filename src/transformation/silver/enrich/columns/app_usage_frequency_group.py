import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class AppUsageFrequencyGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "APP_USAGE_FREQUENCY_PER_WEEK_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("app_usage_frequency_per_week")
            .map_elements(self._classify)
            .alias(self.name().lower())
        )

    def _classify(
        self,
        app_usage_frequency_per_week: int,
    ) -> str:
        if app_usage_frequency_per_week < 0:
            return "Other"
        elif app_usage_frequency_per_week <= 1:
            return "Low"
        elif app_usage_frequency_per_week <= 4:
            return "Moderate"
        return "High"
