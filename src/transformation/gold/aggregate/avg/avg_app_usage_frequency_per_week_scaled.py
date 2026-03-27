from src.transformation.gold.aggregate.median_structure import MedianStructure


class AvgAppUsageFrequencyPerWeekScaled(MedianStructure):
    def __init__(self) -> None:
        super().__init__(column="app_usage_frequency_per_week_scaled")
