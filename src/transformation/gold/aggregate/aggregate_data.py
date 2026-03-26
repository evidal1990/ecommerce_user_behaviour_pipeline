import polars as pl

# Count
from .count.total_users import TotalUsers
from .count.premium_users import PremiumUsers

# Avg
from .avg.avg_daily_session_time_minutes import AvgDailySessionTimeMinutes
from .avg.avg_app_usage_frequency_per_week import AvgAppUsageFrequencyPerWeek
from .avg.avg_product_views_per_day import AvgProductViewsPerDay


class AggregateData:
    def __init__(self) -> None:
        self.df = None

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        self.df = df
        columns = [
            "age_group",
            "gender",
            "country",
            "urban_rural",
            "annual_income_group",
            "education_level",
            "employment_status",
            "device_type",
            "has_children",
        ]
        agg_result = []
        agg_result.append(TotalUsers().aggregate(df=df))
        agg_result.append(PremiumUsers().aggregate(df=df))
        agg_result.append(AvgDailySessionTimeMinutes().aggregate(df=df))
        agg_result.append(AvgAppUsageFrequencyPerWeek().aggregate(df=df))
        agg_result.append(AvgProductViewsPerDay().aggregate(df=df))

        return df.group_by(columns).agg(agg_result).sort(by=columns)
