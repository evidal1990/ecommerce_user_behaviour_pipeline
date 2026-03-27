from src.transformation.gold.aggregate.median_structure import MedianStructure


class AvgCartAbandonmentRate(MedianStructure):
    def __init__(self) -> None:
        super().__init__(column="cart_abandonment_rate")
