from src.transformation.gold.aggregate.sum_structure import SumStructure


class SumDailySessionTimeMinutes(SumStructure):

    def __init__(self) -> None:
        super().__init__(
            column="daily_session_time_minutes", sufix="daily_session_time_minutes"
        )
