import polars as pl
from src.transformation.silver.normalize.normalize import Normalize
from src.transformation.silver.normalize.min_max_strategy import MinMaxScaling


class NormalizeExecutor:
    def __init__(self) -> None:
        pass

    def start(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        return Normalize(
            [
                MinMaxScaling(
                    column="stress_from_financial_decisions_level",
                ),
                MinMaxScaling(
                    column="overall_stress_level",
                ),
                MinMaxScaling(
                    column="sleep_quality_level",
                ),
                MinMaxScaling(
                    column="physical_activity_level",
                ),
                MinMaxScaling(
                    column="brand_loyalty_score",
                ),
                MinMaxScaling(
                    column="impulse_buying_score",
                ),
                MinMaxScaling(
                    column="social_media_influence_score",
                ),
                MinMaxScaling(
                    column="mental_health_score",
                ),
                MinMaxScaling(
                    column="impulse_purchases_per_month",
                ),
                MinMaxScaling(
                    column="checkout_abandonments_per_month",
                ),
                MinMaxScaling(
                    column="product_views_per_day",
                ),
                MinMaxScaling(
                    column="ad_views_per_day",
                ),
                MinMaxScaling(
                    column="social_sharing_frequency_per_year",
                ),
                MinMaxScaling(
                    column="review_writing_frequency_per_year",
                ),
                MinMaxScaling(
                    column="return_frequency_per_year",
                ),
                MinMaxScaling(
                    column="travel_frequency_per_year",
                ),
                MinMaxScaling(
                    column="return_rate",
                ),
                MinMaxScaling(
                    column="purchase_conversion_rate",
                ),
                MinMaxScaling(
                    column="notification_response_rate",
                ),
                MinMaxScaling(
                    column="cart_abandonment_rate",
                ),
                MinMaxScaling(
                    column="browse_to_buy_ratio",
                ),
                MinMaxScaling(
                    column="exercise_frequency_per_week",
                ),
                MinMaxScaling(
                    column="coupon_usage_frequency",
                ),
                MinMaxScaling(
                    column="app_usage_frequency_per_week",
                ),
            ]
        ).execute(df=df)
