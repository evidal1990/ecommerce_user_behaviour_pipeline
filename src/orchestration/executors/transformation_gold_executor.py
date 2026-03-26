import logging
import polars as pl
from typing import Self
from pathlib import Path
from src.transformation.gold.delete.delete import Delete
from src.transformation.gold.aggregate.aggregate_data import AggregateData


class TransformationGoldExecutor:

    def __init__(
        self,
        settings: dict,
    ) -> None:
        data = settings.get("data")
        if not data or "gold" not in data:
            raise ValueError("Configuração de gold não encontrada")

        self._settings = data["gold"]
        self.df = None

    def start(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        logging.info("Transformação de dados provenientes da camada silver iniciada")
        self.df = AggregateData().execute(df)
        self._write_gold()
        logging.info("Transformação de dados provenientes da camada silver finalizada")
        return self.df

    def _delete_unused_columns(self) -> Self:
        df = self.df
        columns = [
            "stress_from_financial_decisions_level",
            "overall_stress_level",
            "sleep_quality_level",
            "physical_activity_level",
            "brand_loyalty_score",
            "impulse_buying_score",
            "social_media_influence_score",
            "mental_health_score",
            "impulse_purchases_per_month",
            "checkout_abandonments_per_month",
            "product_views_per_day",
            "ad_views_per_day",
            "social_sharing_frequency_per_year",
            "review_writing_frequency_per_year",
            "return_frequency_per_year",
            "travel_frequency_per_year",
            "return_rate",
            "purchase_conversion_rate",
            "notification_response_rate",
            "cart_abandonment_rate",
            "browse_to_buy_ratio",
            "exercise_frequency_per_week",
            "coupon_usage_frequency",
            "app_usage_frequency_per_week",
        ]
        self.df = Delete(columns).execute(df)
        return self

    def _write_gold(self) -> None:
        path = self._settings["destination"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.df.write_csv(path)
        #self.df.write_parquet(path, compression="zstd", statistics=True)
