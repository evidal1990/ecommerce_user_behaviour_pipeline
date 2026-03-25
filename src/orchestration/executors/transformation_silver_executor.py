import logging
import polars as pl
from pathlib import Path
# Clean
from src.transformation.silver.clean.clean import CleanData
from src.transformation.silver.clean.format import FormatData
from src.transformation.silver.clean.remove_duplicates import RemoveDuplicates
from src.transformation.silver.clean.fill_columns import FillColumns

# Normalize
from src.transformation.silver.normalize.normalize import Normalize
from src.transformation.silver.normalize.min_max_strategy import MinMaxScaling

# Enrich (core)
from src.transformation.silver.enrich.enrich import EnrichData

# Enrich (columns)
from src.transformation.silver.enrich.columns import (
    CreateIsFutureDateColumn,
    AgeGroup,
    HouseholdSizeGroup,
    BrandLoyaltyScoreGroup,
    ImpulseBuyingScoreGroup,
    SocialMediaInfluenceScoreGroup,
    ExerciseFrequencyGroup,
    StressFromFinancialDecisionsGroup,
    OverallStressLevelGroup,
    PhysicalActivityLevelGroup,
    ReferralCountGroup,
    ImpulsePurchasesPerMonthGroup,
    BrowseToBuyRatioGroup,
    ReturnRateGroup,
    PurchaseConversionRateGroup,
    AppUsageFrequencyGroup,
    NotificationResponseRateGroup,
    SocialSharingFrequencyGroup,
    CartAbandonmentRateGroup,
)

# Utils
from src.utils import file_io

BASE_DIR = Path(__file__).resolve().parents[3]


class TransformationSilverExecutor:
    def __init__(self, settings: dict) -> None:
        data = settings.get("data")
        if not data or "silver" not in data:
            raise ValueError("Configuração de silver não encontrada")

        self._settings = data["silver"]

    def start(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        logging.info("Transformação de dados provenientes da camada bronze iniciada")
        df = self._clean(df=df)
        df = self._normalize(df=df)
        df = self._enrich(df=df)
        self._write_silver(df=df)
        logging.info("Transformação de dados provenientes da camada bronze finalizada")
        return df

    def _clean(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        return CleanData(
            [
                RemoveDuplicates(),
                FormatData(),
                FillColumns(),
            ]
        ).execute(df=df)

    def _normalize(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        return Normalize(
            [
                MinMaxScaling(column="stress_from_financial_decisions_level"),
                MinMaxScaling(column="overall_stress_level"),
                MinMaxScaling(column="sleep_quality_level"),
                MinMaxScaling(column="physical_activity_level"),
                MinMaxScaling(column="brand_loyalty_score"),
                MinMaxScaling(column="impulse_buying_score"),
                MinMaxScaling(column="social_media_influence_score"),
                MinMaxScaling(column="mental_health_score"),
                MinMaxScaling(column="impulse_purchases_per_month"),
                MinMaxScaling(column="checkout_abandonments_per_month"),
                MinMaxScaling(column="product_views_per_day"),
                MinMaxScaling(column="ad_views_per_day"),
                MinMaxScaling(column="social_sharing_frequency_per_year"),
                MinMaxScaling(column="review_writing_frequency_per_year"),
                MinMaxScaling(column="return_frequency_per_year"),
                MinMaxScaling(column="travel_frequency_per_year"),
                MinMaxScaling(column="return_rate"),
                MinMaxScaling(column="purchase_conversion_rate"),
                MinMaxScaling(column="notification_response_rate"),
                MinMaxScaling(column="cart_abandonment_rate"),
                MinMaxScaling(column="browse_to_buy_ratio"),
            ]
        ).execute(df=df)

    def _enrich(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        parent = self._settings["parent"]
        return EnrichData(
            [
                CreateIsFutureDateColumn(settings=parent, column="last_purchase_date"),
                AgeGroup(),
                HouseholdSizeGroup(),
                BrandLoyaltyScoreGroup(),
                ImpulseBuyingScoreGroup(),
                SocialMediaInfluenceScoreGroup(),
                ExerciseFrequencyGroup(),
                StressFromFinancialDecisionsGroup(),
                OverallStressLevelGroup(),
                PhysicalActivityLevelGroup(),
                ReferralCountGroup(),
                ImpulsePurchasesPerMonthGroup(),
                BrowseToBuyRatioGroup(),
                ReturnRateGroup(),
                PurchaseConversionRateGroup(),
                AppUsageFrequencyGroup(),
                NotificationResponseRateGroup(),
                SocialSharingFrequencyGroup(),
                CartAbandonmentRateGroup(),
            ]
        ).execute(df=df)

    def _write_silver(
        self,
        df,
    ) -> None:
        path = self._settings["destination"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(path)

    def _load_contract(self) -> dict:
        path = BASE_DIR.joinpath(
            "src",
            "transformation",
            "silver",
            "schema.yaml",
        )
        try:
            return file_io.read_yaml(path)
        except FileNotFoundError:
            logging.error(f"Schema não encontrado em {path}")
            raise
