from src.transformation.gold.metrics.percentage_structure import PercentageStructure


class PremiumSubscriptionAdoption(PercentageStructure):
    def __init__(self) -> None:
        super().__init__(column="premium_subscription_group")
