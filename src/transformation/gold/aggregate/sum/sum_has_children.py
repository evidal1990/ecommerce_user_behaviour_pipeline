from src.transformation.gold.aggregate.sum_structure import SumStructure


class SumHasChildren(SumStructure):

    def __init__(self) -> None:
        super().__init__(column="has_children", sufix="has_children")
