from src.transformation.gold.metrics.percentage_structure import PercentageStructure


class PercentageUsersByUrbanRural(PercentageStructure):
    def __init__(self) -> None:
        super().__init__(column="urban_rural")
