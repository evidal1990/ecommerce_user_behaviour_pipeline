import polars as pl
from .kpis.percentage.users_by_age_group import PercentageUsersByAgeGroup
from .kpis.percentage.users_by_gender import PercentageUsersByGender


class CreateMetrics:

    def __init__(self) -> None:
        self.df = None

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        kpis = []
        kpis.append(PercentageUsersByAgeGroup().calculate(df=df))
        kpis.append(PercentageUsersByGender().calculate(df=df))

        return pl.concat(kpis)
