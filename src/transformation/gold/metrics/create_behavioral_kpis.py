import polars as pl
from src.transformation.gold.metrics.kpis.behavioral import (
    PremiumSubscriptionAdoption,
    AvgDailySessionTime,
    AvgAppUsageFrequency,
    AvgProductViewsPerDay,
    PreferredProductCategory,
)
from .create_kpis import CreateKpis


class CreateBehavioralKpis(CreateKpis):

    def __init__(self) -> None:
        super().__init__(
            standard_columns=[
                "metric",
                "metric_type",
                "dimension",
                "value",
                "country",
                "age_group",
                "annual_income_group",
                "education_level",
                "device_type",
                "metric_value",
            ],
            kpis=self.build_kpis(
                [
                    {
                        "class": PremiumSubscriptionAdoption,
                        "dimension": "premium_subscription_group",
                        "group_by": [
                            "country",
                            "age_group",
                            "annual_income_group",
                            "education_level",
                            "device_type",
                        ],
                    },
                    {
                        "class": AvgDailySessionTime,
                        "dimension": "country",
                        "group_by": [],
                    },
                    {
                        "class": AvgDailySessionTime,
                        "dimension": "age_group",
                        "group_by": [],
                    },
                    {
                        "class": AvgDailySessionTime,
                        "dimension": "device_type",
                        "group_by": [],
                    },
                    {
                        "class": AvgAppUsageFrequency,
                        "dimension": "country",
                        "group_by": [],
                    },
                    {
                        "class": AvgAppUsageFrequency,
                        "dimension": "age_group",
                        "group_by": [],
                    },
                    {
                        "class": AvgAppUsageFrequency,
                        "dimension": "device_type",
                        "group_by": [],
                    },
                    {
                        "class": AvgProductViewsPerDay,
                        "dimension": "country",
                        "group_by": [],
                    },
                    {
                        "class": AvgProductViewsPerDay,
                        "dimension": "age_group",
                        "group_by": [],
                    },
                    {
                        "class": AvgProductViewsPerDay,
                        "dimension": "device_type",
                        "group_by": [],
                    },
                    {
                        "class": PreferredProductCategory,
                        "dimension": "product_category_preference",
                        "group_by": [
                            "country",
                            "age_group",
                            "device_type",
                        ],
                    },
                ]
            ),
        )
