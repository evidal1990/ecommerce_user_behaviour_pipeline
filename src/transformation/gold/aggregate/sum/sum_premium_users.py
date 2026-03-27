import polars as pl

from src.transformation.gold.aggregate.sum_structure import SumStructure


class SumPremiumUsers(SumStructure):

    def __init__(self) -> None:
        super().__init__(column="premium_subscription", sufix="premium_users")
