import logging
from src.ingestion.csv_ingestion import CsvIngestion
from src.transformation.bronze.structure_data import StructureData
from src.validation.business_rules import BusinessRulesChecks
from src.validation.semantic_rules_validator import SemanticRulesValidator
from src.validation.duplicated_user_id import DuplicatedUserId


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
        SemanticRulesValidator([DuplicatedUserId()]).execute(df)
        logging.info("Validação de regras semânticas finalizada...")

        # logging.info("Validação de regras de negócio iniciada...")
        # BusinessRulesChecks(df).execute()
        # logging.info("Validação de regras de negócio finalizada...")
