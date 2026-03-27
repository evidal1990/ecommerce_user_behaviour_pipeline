from src.transformation.gold.metrics.percentage_structure import PercentageStructure


class PercentageUsersByGender(PercentageStructure):
    def __init__(self) -> None:
        super().__init__(column="gender")
