import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class StressFromFinancialDecisionsGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "STRESS_FROM_FINANCIAL_DECISIONS_LEVEL_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("stress_from_financial_decisions_level")
            .map_elements(self._classify)
            .alias(self.name().lower())
        )

    def _classify(
        self,
        stress_from_financial_decisions_level: int,
    ) -> str:
        if stress_from_financial_decisions_level < 0:
            return "Other"
        elif stress_from_financial_decisions_level <= 2:
            return "Very Low"
        elif stress_from_financial_decisions_level <= 4:
            return "Low"
        elif stress_from_financial_decisions_level <= 6:
            return "Moderate"
        elif stress_from_financial_decisions_level <= 8:
            return "High"
        return "Very High"
