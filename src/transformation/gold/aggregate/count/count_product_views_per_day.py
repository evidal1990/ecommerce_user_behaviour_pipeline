from src.transformation.gold.aggregate.count_structure import CountStructure


class CountProductViewsPerDay(CountStructure):

    def __init__(self) -> None:
        super().__init__(column="user_id", sufix="product_views_per_day")
