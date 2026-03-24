import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class AgeGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "AGE_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("age").map_elements(self._classify).alias(self.name().lower())
        )

    def _classify(
        self,
        age: int,
    ) -> str:
        if age < 18:
            return "Other"
        elif age <= 24:
            return "Early Adopters"
        elif age <= 34:
            return "Early Career Professionals"
        elif age <= 44:
            return "Professional Consolidation"
        elif age <= 54:
            return "High Financial Stability"
        elif age <= 64:
            return "Pre-Retirement"
        return "Low Digital Adoption"
