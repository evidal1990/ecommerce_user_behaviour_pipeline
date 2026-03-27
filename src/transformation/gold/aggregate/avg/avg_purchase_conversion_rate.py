from src.transformation.gold.aggregate.median_structure import MedianStructure


class AvgPurchaseConversionRate(MedianStructure):
    def __init__(self) -> None:
        super().__init__(column="purchase_conversion_rate")
