import polars as pl

from src.transformation.gold.aggregate.sum_structure import SumStructure


class SumMonthlySpend(SumStructure):

    def __init__(self) -> None:
        super().__init__(column="monthly_spend", sufix="monthly_spend")
