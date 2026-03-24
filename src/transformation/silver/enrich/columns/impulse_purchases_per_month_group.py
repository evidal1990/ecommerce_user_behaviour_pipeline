import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class ImpulsePurchasesPerMonthGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "IMPULSE_PURCHASES_PER_MONTH_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("impulse_purchases_per_month")
            .map_elements(self._classify)
            .alias(self.name().lower())
        )

    def _classify(
        self,
        impulse_purchases_per_month: int,
    ) -> str:
        if impulse_purchases_per_month < 0:
            return "Other"
        elif impulse_purchases_per_month == 0:
            return "Non-impulsive"
        elif impulse_purchases_per_month <= 2:
            return "Not Very Impulsive"
        elif impulse_purchases_per_month <= 5:
            return "Moderate"
        return "Impulsilve"
