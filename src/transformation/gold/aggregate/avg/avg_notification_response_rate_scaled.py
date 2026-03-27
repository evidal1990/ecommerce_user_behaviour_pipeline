from src.transformation.gold.aggregate.median_structure import MedianStructure


class AvgNotificationResponseRateScaled(MedianStructure):
    def __init__(self) -> None:
        super().__init__(column="notification_response_rate_scaled")
