import logging
from datetime import datetime
from consts.rule_type import RuleType
from src.ingestion.csv_ingestion import CsvIngestion
from src.transformation.bronze.structure_data import StructureData
from src.validation import RulesValidator, DtypeValidator, DataFrameValidator
from src.validation.semantic import (
    DuplicatedUserId,
    FutureDates,
    MinValue,
    EmployedWithoutIncome,
    UnemployedUserWithIncome,
    SelfEmployedWithoutIncome,
)
from src.validation.business import AllowedMinMaxValues, AllowedColumnValues
from src.validation.quality import NotAllowedNullCount, RequiredColumns, ColumnDType


class Pipeline:
    def __init__(self, settings: dict) -> None:
        """
        Inicializa o objeto Pipeline com as configura es fornecidas.

        Parametros:
            settings (dict): Configura es do pipeline.

        Retorno:
            None
        """
        self.settings = settings

    def run(self) -> None:
        """
        Executa o pipeline de ingestão de CSV.

        Inicia a ingestão de CSV com as configurações fornecidas,
        executa a classe CsvIngestion e registra logs de início e fim
        da execução.

        Executa o pipeline de transformação dos dados (bronze)

        Inicia a transformação dos dados com as configurações fornecidas,
        executa a classe StructureData e registra logs de início e fim
        da execução.

        Retorno:
            None
        """
        logging.info("Ingestão de CSV iniciada...")
        df = CsvIngestion(self.settings).execute()
        logging.info("Ingestão de CSV finalizada...\n")

        logging.info("Validação de colunas do dataframe iniciada...")
        DataFrameValidator(RuleType.QUALITY, [RequiredColumns()]).execute(df)
        logging.info("Validação de colunas do dataframe finalizada...\n")

        logging.info("Validação de tipos das colunas do dataframe iniciada...")
        DtypeValidator(
            RuleType.QUALITY,
            [
                ColumnDType(column="user_id"),
                ColumnDType(column="age"),
                ColumnDType(column="gender"),
                ColumnDType(column="country"),
                ColumnDType(column="urban_rural"),
                ColumnDType(column="income_level"),
                ColumnDType(column="employment_status"),
                ColumnDType(column="education_level"),
                ColumnDType(column="relationship_status"),
                ColumnDType(column="has_children"),
                ColumnDType(column="household_size"),
                ColumnDType(column="occupation"),
                ColumnDType(column="ethnicity"),
                ColumnDType(column="language_preference"),
                ColumnDType(column="device_type"),
                ColumnDType(column="budgeting_style"),
                ColumnDType(column="brand_loyalty_score"),
                ColumnDType(column="impulse_buying_score"),
                ColumnDType(column="environmental_consciousness"),
                ColumnDType(column="health_conscious_shopping"),
                ColumnDType(column="travel_frequency"),
                ColumnDType(column="hobby_count"),
                ColumnDType(column="social_media_influence_score"),
                ColumnDType(column="reading_habits"),
                ColumnDType(column="exercise_frequency"),
                ColumnDType(column="stress_from_financial_decisions"),
                ColumnDType(column="overall_stress_level"),
                ColumnDType(column="sleep_quality"),
                ColumnDType(column="physical_activity_level"),
                ColumnDType(column="mental_health_score"),
                ColumnDType(column="weekly_purchases"),
                ColumnDType(column="monthly_spend"),
                ColumnDType(column="average_order_value"),
                ColumnDType(column="preferred_payment_method"),
                ColumnDType(column="coupon_usage_frequency"),
                ColumnDType(column="loyalty_program_member"),
                ColumnDType(column="referral_count"),
                ColumnDType(column="product_category_preference"),
                ColumnDType(column="shopping_time_of_day"),
                ColumnDType(column="weekend_shopper"),
                ColumnDType(column="impulse_purchases_per_month"),
                ColumnDType(column="browse_to_buy_ratio"),
                ColumnDType(column="return_frequency"),
                ColumnDType(column="return_rate"),
                ColumnDType(column="daily_session_time_minutes"),
                ColumnDType(column="product_views_per_day"),
                ColumnDType(column="ad_views_per_day"),
                ColumnDType(column="wishlist_items_count"),
                ColumnDType(column="cart_items_average"),
                ColumnDType(column="checkout_abandonments_per_month"),
                ColumnDType(column="purchase_conversion_rate"),
                ColumnDType(column="app_usage_frequency"),
                ColumnDType(column="notification_response_rate"),
                ColumnDType(column="account_age_months"),
                ColumnDType(column="last_purchase_date"),
                ColumnDType(column="social_sharing_frequency"),
                ColumnDType(column="premium_subscription"),
                ColumnDType(column="cart_abandonment_rate"),
                ColumnDType(column="review_writing_frequency"),
            ],
        ).execute(df)
        logging.info("Validação de tipos das colunas do dataframe finalizada...\n")

        logging.info("Validação de regras de qualidade iniciada...")
        RulesValidator(
            RuleType.QUALITY,
            [
                NotAllowedNullCount(column="user_id"),
                NotAllowedNullCount(column="age"),
                NotAllowedNullCount(column="gender"),
                NotAllowedNullCount(column="country"),
                NotAllowedNullCount(column="urban_rural"),
                NotAllowedNullCount(column="income_level"),
                NotAllowedNullCount(column="employment_status"),
                NotAllowedNullCount(column="education_level"),
                NotAllowedNullCount(column="relationship_status"),
                NotAllowedNullCount(column="has_children"),
                NotAllowedNullCount(column="household_size"),
                NotAllowedNullCount(column="occupation"),
                NotAllowedNullCount(column="device_type"),
                NotAllowedNullCount(column="brand_loyalty_score"),
                NotAllowedNullCount(column="weekly_purchases"),
                NotAllowedNullCount(column="monthly_spend"),
                NotAllowedNullCount(column="preferred_payment_method"),
                NotAllowedNullCount(column="coupon_usage_frequency"),
                NotAllowedNullCount(column="referral_count"),
                NotAllowedNullCount(column="product_category_preference"),
                NotAllowedNullCount(column="impulse_purchases_per_month"),
                NotAllowedNullCount(column="return_frequency"),
                NotAllowedNullCount(column="daily_session_time_minutes"),
                NotAllowedNullCount(column="product_views_per_day"),
                NotAllowedNullCount(column="has_children"),
                NotAllowedNullCount(column="checkout_abandonments_per_month"),
                NotAllowedNullCount(column="purchase_conversion_rate"),
                NotAllowedNullCount(column="app_usage_frequency"),
                NotAllowedNullCount(column="notification_response_rate"),
                NotAllowedNullCount(column="account_age_months"),
                NotAllowedNullCount(column="last_purchase_date"),
                NotAllowedNullCount(column="premium_subscription"),
                NotAllowedNullCount(column="cart_abandonment_rate"),
            ],
        ).execute(df)
        logging.info("Validação de regras de qualidade finalizada...\n")

        logging.info("Estruturação de dados brutos iniciada...")
        df = StructureData(self.settings).execute()
        logging.info("Estruturação de dados brutos finalizada...\n")

        logging.info("Validação de regras semânticas iniciada...")
        semantic_rules_validation_result = RulesValidator(
            RuleType.SEMANTIC,
            [
                DuplicatedUserId(),
                FutureDates(
                    column="last_purchase_date", date_limit=datetime.now().date()
                ),
                MinValue(column="annual_income", min_limit=0.0),
                MinValue(column="household_size", min_limit=0),
                MinValue(column="monthly_spend", min_limit=0.0),
                MinValue(column="average_order_value", min_limit=0),
                MinValue(column="daily_session_time_minutes", min_limit=0),
                MinValue(column="cart_items_average", min_limit=0),
                MinValue(column="account_age_months", min_limit=0),
                EmployedWithoutIncome(),
                SelfEmployedWithoutIncome(),
                UnemployedUserWithIncome(),
            ]
        ).execute(df)
        logging.info("Validação de regras semânticas finalizada...\n")

        logging.info("Validação de regras de negócio iniciada...")
        # RulesValidator(
        #     RuleType.BUSINESS,
        #     [
        #         AllowedMinMaxValues(column="brand_loyalty_score"),
        #         AllowedMinMaxValues(column="browse_to_buy_ratio"),
        #         AllowedMinMaxValues(column="cart_abandonment_rate"),
        #         AllowedMinMaxValues(column="checkout_abandonments_per_month"),
        #         AllowedMinMaxValues(column="exercise_frequency_per_week"),
        #         AllowedMinMaxValues(column="impulse_buying_score"),
        #         AllowedMinMaxValues(column="impulse_purchases_per_month"),
        #         AllowedMinMaxValues(column="mental_health_score"),
        #         AllowedMinMaxValues(column="notification_response_rate"),
        #         AllowedMinMaxValues(column="overall_stress_level"),
        #         AllowedMinMaxValues(column="physical_activity_level"),
        #         AllowedMinMaxValues(column="purchase_conversion_rate"),
        #         AllowedMinMaxValues(column="reading_habits_per_month"),
        #         AllowedMinMaxValues(column="return_rate"),
        #         AllowedMinMaxValues(column="sleep_quality_level"),
        #         AllowedMinMaxValues(column="social_media_influence_score"),
        #         AllowedMinMaxValues(column="stress_from_financial_decisions_level"),
        #         AllowedMinMaxValues(column="travel_frequency_per_year"),
        #         AllowedColumnValues(column="budgeting_style"),
        #         AllowedColumnValues(column="device_type"),
        #         AllowedColumnValues(column="education_level"),
        #         AllowedColumnValues(column="employment_status"),
        #         AllowedColumnValues(column="ethnicity"),
        #         AllowedColumnValues(column="gender"),
        #         AllowedColumnValues(column="has_children"),
        #         AllowedColumnValues(column="has_environmental_consciousness"),
        #         AllowedColumnValues(column="has_health_conscious_shopping"),
        #         AllowedColumnValues(column="is_loyalty_program_member"),
        #         AllowedColumnValues(column="is_weekend_shopper"),
        #         AllowedColumnValues(column="preferred_payment_method"),
        #         AllowedColumnValues(column="product_category_preference"),
        #         AllowedColumnValues(column="relationship_status"),
        #         AllowedColumnValues(column="shopping_time_of_day"),
        #         AllowedColumnValues(column="urban_rural"),
        #     ],
        # ).execute(df)
        logging.info("Validação de regras de negócio finalizada...\n")
