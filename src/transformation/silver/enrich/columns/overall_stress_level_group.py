import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class OverallStressLevelGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "OVERALL_STRESS_LEVEL_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("overall_stress_level")
            .map_elements(self._classify)
            .alias(self.name().lower())
        )

    def _classify(
        self,
        overall_stress_level: int,
    ) -> str:
        if overall_stress_level < 0:
            return "Other"
        elif overall_stress_level <= 2:
            return "Very Low"
        elif overall_stress_level <= 4:
            return "Low"
        elif overall_stress_level <= 6:
            return "Moderate"
        elif overall_stress_level <= 8:
            return "High"
        return "Very High"
