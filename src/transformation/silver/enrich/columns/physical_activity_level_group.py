import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class PhysicalActivityLevelGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "PHYSICAL_ACTIVITY_LEVEL_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("physical_activity_level")
            .map_elements(self._classify)
            .alias(self.name().lower())
        )

    def _classify(
        self,
        physical_activity_level: int,
    ) -> str:
        if physical_activity_level < 0:
            return "Other"
        elif physical_activity_level <= 2:
            return "Very Light"
        elif physical_activity_level <= 4:
            return "Light"
        elif physical_activity_level <= 6:
            return "Moderate"
        elif physical_activity_level <= 8:
            return "Heavy"
        return "Very Heavy"
