import polars as pl
from src.transformation.gold.metrics.kpis.behavioral import (
    PremiumSubscriptionAdoption,
    AvgDailySessionTime,
    AvgAppUsageFrequency,
    AvgProductViewsPerDay,
    PreferredProductCategory,
    PreferredPaymentMethod,
    AvgBrandLoyaltyScore,
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
                "cart_abandonment_rate_group",
                "avg_brand_loyalty_score",
                "metric_value",
            ],
            kpis=self.build_kpis(
                [
                    {
                        "class": PremiumSubscriptionAdoption,
                        "dimensions": ["premium_subscription_group"],
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
                        "dimensions": [
                            "country",
                            "age_group",
                            "device_type",
                        ],
                        "group_by": [],
                    },
                    {
                        "class": AvgAppUsageFrequency,
                        "dimensions": [
                            "country",
                            "age_group",
                            "device_type",
                        ],
                        "group_by": [],
                    },
                    {
                        "class": AvgProductViewsPerDay,
                        "dimensions": [
                            "country",
                            "age_group",
                            "device_type",
                        ],
                        "group_by": [],
                    },
                    {
                        "class": PreferredProductCategory,
                        "dimensions": ["product_category_preference"],
                        "group_by": [
                            "country",
                            "age_group",
                            "device_type",
                        ],
                    },
                    {
                        "class": PreferredPaymentMethod,
                        "dimensions": ["preferred_payment_method"],
                        "group_by": [
                            "country",
                            "age_group",
                            "annual_income_group",
                            "device_type",
                            "cart_abandonment_rate_group",
                        ],
                    },
                    {
                        "class": AvgBrandLoyaltyScore,
                        "dimensions": [
                            "country",
                            "age_group",
                            "annual_income_group",
                            "education_level",
                            "premium_subscription_group",
                        ],
                        "group_by": [],
                    },
                ]
            ),
        )
