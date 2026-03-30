import polars as pl

# Count
from .count.count_total_users import CountTotalUsers
from .count.count_product_views_per_day import CountProductViewsPerDay
from .count.count_last_purchase_date import CountLastPurchaseDate

# Sum
from .sum.sum_premium_users import SumPremiumUsers
from .sum.sum_monthly_spend import SumMonthlySpend
from .sum.sum_has_children import SumHasChildren
from .sum.sum_checkout_abandonments_per_month import SumCheckoutAbandonmentsPerMonth
from .sum.sum_daily_session_time_minutes import SumDailySessionTimeMinutes

# Avg
from .avg.avg_daily_session_time_minutes import AvgDailySessionTimeMinutes
from .avg.avg_app_usage_frequency_per_week import AvgAppUsageFrequencyPerWeek
from .avg.avg_app_usage_frequency_per_week_scaled import (
    AvgAppUsageFrequencyPerWeekScaled,
)
from .avg.avg_product_views_per_day import AvgProductViewsPerDay
from .avg.avg_product_views_per_day_scaled import AvgProductViewsPerDayScaled
from .avg.avg_brand_loyalty_score import AvgBrandLoyaltyScore
from .avg.avg_coupon_usage_frequency import AvgCouponUsageFrequency
from .avg.avg_referral_count import AvgReferralCount
from .avg.avg_purchase_conversion_rate import AvgPurchaseConversionRate
from .avg.avg_cart_abandonment_rate import AvgCartAbandonmentRate
from .avg.avg_notification_response_rate_scaled import AvgNotificationResponseRateScaled


class AggregateData:
    def __init__(self) -> None:
        self.df = None

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        self.df = df
        columns = [
            "gender",
            "country",
            "urban_rural",
            "education_level",
            "employment_status",
            "device_type",
            "preferred_payment_method",
            "age_group",
            "annual_income_group",
            "household_size_group",
            "brand_loyalty_score_group",
            "impulse_buying_score_group",
            "social_media_influence_score_group",
            "stress_from_financial_decisions_level_group",
            "referral_count_group",
            "impulse_purchases_per_month_group",
            "browse_to_buy_ratio_group",
            "return_rate_group",
            "purchase_conversion_rate_group",
            "cart_abandonment_rate_group",
            "app_usage_frequency_per_week_group",
            "has_children_group",
            "premium_subscription_group",
        ]
        agg_result = []
        # Count
        agg_result.append(CountTotalUsers().aggregate())
        agg_result.append(CountProductViewsPerDay().aggregate())
        agg_result.append(CountLastPurchaseDate().aggregate())

        # Sum
        agg_result.append(SumPremiumUsers().aggregate())
        agg_result.append(SumMonthlySpend().aggregate())
        agg_result.append(SumHasChildren().aggregate())
        agg_result.append(SumCheckoutAbandonmentsPerMonth().aggregate())
        agg_result.append(SumDailySessionTimeMinutes().aggregate())

        # Avg
        agg_result.append(AvgDailySessionTimeMinutes().aggregate())
        agg_result.append(AvgAppUsageFrequencyPerWeek().aggregate())
        agg_result.append(AvgAppUsageFrequencyPerWeekScaled().aggregate())
        agg_result.append(AvgProductViewsPerDay().aggregate())
        agg_result.append(AvgProductViewsPerDayScaled().aggregate())
        agg_result.append(AvgBrandLoyaltyScore().aggregate())
        agg_result.append(AvgCouponUsageFrequency().aggregate())
        agg_result.append(AvgReferralCount().aggregate())
        agg_result.append(AvgPurchaseConversionRate().aggregate())
        agg_result.append(AvgCartAbandonmentRate().aggregate())
        agg_result.append(AvgNotificationResponseRateScaled().aggregate())

        return df.group_by(columns).agg(agg_result).sort(by=columns)
