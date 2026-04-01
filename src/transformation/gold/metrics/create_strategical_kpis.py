from src.transformation.gold.metrics.kpis.strategical import (
    AvgPurchaseConversionRate,
    AvgCartAbandonmentRate,
    ChurnRate,
    DailyActiveUsers,
    NetPromoterScore
)
from .create_kpis import CreateKpis


class CreateStrategicalKpis(CreateKpis):

    def __init__(self) -> None:
        super().__init__(
            standard_columns=[
                "metric",
                "metric_type",
                "dimension",
                "value",
                "country",
                "annual_income_group",
                "device_type",
                "premium_subscription_group",
                "brand_loyalty_score_group",
                "preferred_payment_method",
                "social_sharing_frequency_per_year_group",
                "app_usage_frequency_per_week_group",
                "stress_from_financial_decisions_level_group",
                "return_rate_group",
                "impulse_buying_score_group",
                "browse_to_buy_ratio_group",
                "age_group",
                "purchase_conversion_rate_group",
                "metric_value",
            ],
            kpis=self.build_kpis(
                [
                    {
                        "class": AvgPurchaseConversionRate,
                        "dimensions": [
                            "country",
                            "device_type",
                            "app_usage_frequency_per_week_group",
                            "brand_loyalty_score_group",
                            "browse_to_buy_ratio_group",
                            "social_sharing_frequency_per_year_group",
                        ],
                        "group_by": [],
                    },
                    {
                        "class": AvgCartAbandonmentRate,
                        "dimensions": [
                            "annual_income_group",
                            "preferred_payment_method",
                            "stress_from_financial_decisions_level_group",
                        ],
                        "group_by": [],
                    },
                    {
                        "class": ChurnRate,
                        "dimensions": ["last_purchase_date"],
                        "group_by": [
                            "country",
                            "age_group",
                            "device_type",
                            "premium_subscription_group",
                            "return_rate_group",
                            "impulse_buying_score_group",
                            "app_usage_frequency_per_week_group",
                            "brand_loyalty_score_group",
                        ],
                    },
                    {
                        "class": DailyActiveUsers,
                        "dimensions": [
                            "country",
                            "age_group",
                            "premium_subscription_group",
                            "device_type",
                            "brand_loyalty_score_group",
                        ],
                        "group_by": [],
                    },
                    {
                        "class": NetPromoterScore,
                        "dimensions": [
                            "brand_loyalty_score_group",
                        ],
                        "group_by": [
                            "country",
                            "age_group",
                            "purchase_conversion_rate_group",
                            "browse_to_buy_ratio_group",
                            "impulse_buying_score_group",
                            "return_rate_group",
                        ],
                    },
                ]
            ),
        )
