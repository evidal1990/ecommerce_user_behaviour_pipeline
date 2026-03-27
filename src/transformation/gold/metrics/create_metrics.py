import polars as pl
from .kpis.percentage.users_by_age_group import PercentageUsersByAgeGroup
from .kpis.percentage.users_by_gender import PercentageUsersByGender
from .kpis.percentage.users_by_country import PercentageUsersByCountry
from .kpis.percentage.users_by_urban_rural import PercentageUsersByUrbanRural
from .kpis.percentage.users_by_annual_income import PercentageUsersByAnnualIncome
from .kpis.percentage.users_by_education_level import PercentageUsersByEducationLevel
from .kpis.percentage.users_by_employment_status import PercentageUsersByEmploymentStatus
from .kpis.percentage.users_by_device_type import PercentageUsersByDeviceType
from .kpis.percentage.users_by_has_children import PercentageUsersByHasChildren


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
        kpis.append(PercentageUsersByCountry().calculate(df=df))
        kpis.append(PercentageUsersByUrbanRural().calculate(df=df))
        kpis.append(PercentageUsersByAnnualIncome().calculate(df=df))
        kpis.append(PercentageUsersByEducationLevel().calculate(df=df))
        kpis.append(PercentageUsersByEmploymentStatus().calculate(df=df))
        kpis.append(PercentageUsersByDeviceType().calculate(df=df))
        kpis.append(PercentageUsersByHasChildren().calculate(df=df))

        return pl.concat(kpis)
