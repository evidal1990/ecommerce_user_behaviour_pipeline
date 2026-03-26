import polars as pl


class AggregateData:
    def __init__(self) -> None:
        pass

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        columns = [
            "age_group",
            "household_size_group",
            "brand_loyalty_score_group",
            "impulse_buying_score_group",
            "social_media_influence_score_group",
            "exercise_frequency_per_week_group",
            "stress_from_financial_decisions_level_group",
            "overall_stress_level_group",
            "physical_activity_level_group",
            "referral_count_group",
            "impulse_purchases_per_month_group",
            "browse_to_buy_ratio_group",
            "return_rate_group",
            "purchase_conversion_rate_group",
            "app_usage_frequency_per_week_group",
            "notification_response_rate_group",
            "social_sharing_frequency_per_year_group",
            "cart_abandonment_rate_group",
            "review_writing_frequency_per_year_group",
            "gender",
            "country",
            "urban_rural",
            "education_level",
            "employment_status",
            "relationship_status",
            "occupation",
            "ethnicity",
            "language_preference",
            "device_type",
            "budgeting_style",
            "preferred_payment_method",
            "is_loyalty_program_member",
            "product_category_preference",
            "shopping_time_of_day",
            "is_weekend_shopper",
            "premium_subscription",
        ]
        return df.group_by(columns).agg(
            self._statistics(
                [
                    "age",
                    "annual_income",
                    "ad_views_per_day",
                    "account_age_months",
                    "app_usage_frequency_per_week",
                    "average_order_value",
                    "browse_to_buy_ratio",
                    "brand_loyalty_score",
                    "checkout_abandonments_per_month",
                    "coupon_usage_frequency",
                    "cart_abandonment_rate",
                    "daily_session_time_minutes",
                    "exercise_frequency_per_week",
                    "has_children",
                    "household_size",
                    "hobby_count",
                    "impulse_purchases_per_month",
                    "impulse_buying_score",
                    "mental_health_score",
                    "monthly_spend",
                    "notification_response_rate",
                    "overall_stress_level",
                    "purchase_conversion_rate",
                    "product_views_per_day",
                    "physical_activity_level",
                    "reading_habits_per_month",
                    "return_frequency_per_year",
                    "return_rate",
                    "referral_count",
                    "review_writing_frequency_per_year",
                    "social_media_influence_score",
                    "stress_from_financial_decisions_level",
                    "sleep_quality_level",
                    "social_sharing_frequency_per_year",
                    "travel_frequency_per_year",
                    "weekly_purchases",
                ]
            )
        )

    def _statistics(
        self,
        columns: list[str],
    ) -> list[pl.Expr]:
        return [
            expr
            for col in columns
            for expr in [
                pl.col(col).min().alias(f"{col}_min"),
                pl.col(col).max().alias(f"{col}_max"),
                pl.col(col).mean().alias(f"{col}_avg"),
                pl.col(col).median().alias(f"{col}_median"),
            ]
        ]
