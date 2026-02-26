import logging
from datetime import datetime
from src.ingestion.csv_ingestion import CsvIngestion
from src.transformation.bronze.structure_data import StructureData
from src.validation.semantic_rules_validator import SemanticRulesValidator
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
        semantic_rules_validation_result = SemanticRulesValidator(
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
        logging.info("Validação de regras semânticas finalizada...")

        # logging.info("Validação de regras de negócio iniciada...")
        # BusinessRulesChecks(df).execute()
        # logging.info("Validação de regras de negócio finalizada...")
