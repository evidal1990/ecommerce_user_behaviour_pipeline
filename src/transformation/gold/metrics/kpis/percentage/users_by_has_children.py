from src.transformation.gold.metrics.percentage_structure import PercentageStructure


class PercentageUsersByHasChildren(PercentageStructure):
    def __init__(self) -> None:
        super().__init__(column="has_children_group")
