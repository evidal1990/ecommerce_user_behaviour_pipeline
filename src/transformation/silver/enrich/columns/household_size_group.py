import logging
from os import name
import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class HouseholdSizeGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "HOUSEHOLD_SIZE_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("household_size")
            .map_elements(self._classify)
            .alias("household_size_group")
        )

    def _classify(
        self,
        household_size: int,
    ) -> str:
        if household_size < 1:
            return "Other"
        elif household_size == 1:
            return "Single-person"
        elif household_size <= 3:
            return "Small"
        elif household_size <= 5:
            return "Medium"
        return "Large"
