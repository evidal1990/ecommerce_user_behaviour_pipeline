import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class CartAbandonmentRateGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "CART_ABANDONMENT_RATE_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("cart_abandonment_rate")
            .map_elements(self._classify)
            .alias(self.name().lower())
        )

    def _classify(
        self,
        cart_abandonment_rate: int,
    ) -> str:
        if cart_abandonment_rate < 0:
            return "Other"
        elif cart_abandonment_rate <= 20:
            return "Very Low"
        elif cart_abandonment_rate <= 40:
            return "Low"
        elif cart_abandonment_rate <= 60:
            return "Moderate"
        elif cart_abandonment_rate <= 80:
            return "High"
        return "Very High"
