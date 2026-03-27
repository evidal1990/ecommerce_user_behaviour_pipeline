from src.transformation.gold.metrics.percentage_structure import PercentageStructure


class PercentageUsersByAgeGroup(PercentageStructure):
    def __init__(self) -> None:
        super().__init__(column="age_group")
