import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class HasChildrenGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "has_children"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return super().aggregate(df=df, column=self.column)
