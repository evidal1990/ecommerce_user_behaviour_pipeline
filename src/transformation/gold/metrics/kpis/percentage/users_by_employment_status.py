from src.transformation.gold.metrics.percentage_structure import PercentageStructure


class PercentageUsersByEmploymentStatus(PercentageStructure):
    def __init__(self) -> None:
        super().__init__(column="employment_status")
