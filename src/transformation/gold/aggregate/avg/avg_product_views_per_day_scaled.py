from src.transformation.gold.aggregate.median_structure import MedianStructure


class AvgProductViewsPerDayScaled(MedianStructure):
    def __init__(self) -> None:
        super().__init__(column="product_views_per_day_scaled")
