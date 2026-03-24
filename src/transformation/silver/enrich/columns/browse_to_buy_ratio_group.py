import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class BrowseToBuyRatioGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "BROWSE_TO_BUY_RATIO_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("browse_to_buy_ratio")
            .map_elements(self._classify)
            .alias(self.name().lower())
        )

    def _classify(
        self,
        browse_to_buy_ratio: int,
    ) -> str:
        if browse_to_buy_ratio < 0:
            return "Other"
        elif browse_to_buy_ratio <= 20:
            return "Browse a Lot & Buy Little"
        elif browse_to_buy_ratio <= 40:
            return "Low Intention"
        elif browse_to_buy_ratio <= 60:
            return "Undecided"
        elif browse_to_buy_ratio <= 80:
            return "Good Intention"
        return "High Intention"
