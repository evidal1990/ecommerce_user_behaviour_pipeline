from src.transformation.gold.metrics.percentage_structure import PercentageStructure


class PercentageUsersByAnnualIncome(PercentageStructure):
    def __init__(self) -> None:
        super().__init__(column="annual_income_group")
