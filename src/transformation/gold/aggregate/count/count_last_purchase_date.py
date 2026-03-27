from src.transformation.gold.aggregate.count_structure import CountStructure


class CountLastPurchaseDate(CountStructure):

    def __init__(self) -> None:
        super().__init__(column="user_id", sufix="last_purchase_date")
