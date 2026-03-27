from src.transformation.gold.aggregate.sum_structure import SumStructure


class SumCheckoutAbandonmentsPerMonth(SumStructure):

    def __init__(self) -> None:
        super().__init__(
            column="checkout_abandonments_per_month",
            sufix="checkout_abandonments_per_month",
        )
