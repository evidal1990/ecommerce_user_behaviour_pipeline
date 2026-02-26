import logging
from datetime import datetime
from consts.rule_type import RuleType
from src.ingestion.csv_ingestion import CsvIngestion
from src.transformation.bronze.structure_data import StructureData
from src.validation.rules_validator import RulesValidator
from src.validation.semantic.duplicated_user_id import DuplicatedUserId
from src.validation.semantic.future_dates import FutureDates
from src.validation.semantic.min_value import MinValue
from src.validation.semantic.employed_without_income import (
    EmployedWithoutIncome,
)
from src.validation.semantic.unemployed_with_income import (
    UnemployedUserWithIncome,
)
from src.validation.semantic.self_employed_without_income import (
    SelfEmployedWithoutIncome,
)
from src.validation.business.allowed_min_values import AllowedMinValues
from src.validation.business.allowed_max_values import AllowedMaxValues


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
        CsvIngestion(self.settings).execute()
        logging.info("Ingestão de CSV finalizada...")

        logging.info("Estruturação de dados brutos iniciada...")
        df = StructureData(self.settings).execute()
        logging.info("Estruturação de dados brutos finalizada...")

        logging.info("Validação de regras semânticas iniciada...")
        # semantic_rules_validation_result = RulesValidator(
        #     RuleType.SEMANTIC,
        #     [
        #         DuplicatedUserId(),
        #         FutureDates(
        #             column="last_purchase_date", date_limit=datetime.now().date()
        #         ),
        #         MinValue(column="annual_income", min_limit=0.0),
        #         MinValue(column="household_size", min_limit=0),
        #         MinValue(column="monthly_spend", min_limit=0.0),
        #         MinValue(column="average_order_value", min_limit=0),
        #         MinValue(column="daily_session_time_minutes", min_limit=0),
        #         MinValue(column="cart_items_average", min_limit=0),
        #         MinValue(column="account_age_months", min_limit=0),
        #         EmployedWithoutIncome(),
        #         SelfEmployedWithoutIncome(),
        #         UnemployedUserWithIncome(),
        #     ]
        # ).execute(df)
        logging.info("Validação de regras semânticas finalizada...")

        logging.info("Validação de regras de negócio iniciada...")
        RulesValidator(
            RuleType.BUSINESS, [
                AllowedMinValues(column="brand_loyalty_score"),
                AllowedMaxValues(column="brand_loyalty_score"),
                AllowedMinValues(column="browse_to_buy_ratio"),
                AllowedMaxValues(column="browse_to_buy_ratio"),
                AllowedMinValues(column="cart_abandonment_rate"),
                AllowedMaxValues(column="cart_abandonment_rate"),
                AllowedMinValues(column="checkout_abandonments_per_month"),
                AllowedMaxValues(column="checkout_abandonments_per_month"),
                AllowedMinValues(column="exercise_frequency_per_week"),
                AllowedMaxValues(column="exercise_frequency_per_week"),
                AllowedMinValues(column="impulse_buying_score"),
                AllowedMaxValues(column="impulse_buying_score"),
                AllowedMinValues(column="impulse_purchases_per_month"),
                AllowedMaxValues(column="impulse_purchases_per_month"),
                AllowedMinValues(column="mental_health_score"),
                AllowedMaxValues(column="mental_health_score"),
                AllowedMinValues(column="notification_response_rate"),
                AllowedMaxValues(column="notification_response_rate"),
                AllowedMinValues(column="overall_stress_level"),
                AllowedMaxValues(column="overall_stress_level"),
                AllowedMinValues(column="physical_activity_level"),
                AllowedMaxValues(column="physical_activity_level"),
                AllowedMinValues(column="purchase_conversion_rate"),
                AllowedMaxValues(column="purchase_conversion_rate"),
                AllowedMinValues(column="reading_habits_per_month"),
                AllowedMaxValues(column="reading_habits_per_month"),
                AllowedMinValues(column="return_rate"),
                AllowedMaxValues(column="return_rate"),
                AllowedMinValues(column="sleep_quality_level"),
                AllowedMaxValues(column="sleep_quality_level"),
                AllowedMinValues(column="social_media_influence_score"),
                AllowedMaxValues(column="social_media_influence_score"),
                AllowedMinValues(column="stress_from_financial_decisions_level"),
                AllowedMaxValues(column="stress_from_financial_decisions_level"),
                AllowedMinValues(column="travel_frequency_per_year"),
                AllowedMaxValues(column="travel_frequency_per_year"),
                ]
        ).execute(df)
        logging.info("Validação de regras de negócio finalizada...")
