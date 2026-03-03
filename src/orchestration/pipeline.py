import logging
from .executors.ingestion_executor import IngestionExecutor
from .executors.dataframe_validation_executor import DataFrameValidatorExecutor
from .executors.data_structuring_executor import DataStructuringExecutor
from .executors.semantic_rules_executor import SemanticRulesExecutor


class Pipeline:
    def __init__(self, settings: dict) -> None:
        """
        Inicializa o objeto Pipeline com as configura es fornecidas.

        Parametros:
            settings (dict): Configura es do pipeline.

        Retorno:
            None
        """
        self.df = None
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
        logging.info("Pipeline iniciada")
        df_after_ingestion = IngestionExecutor(
            settings=self.settings,
        ).execute()
        DataFrameValidatorExecutor().execute(
            df=df_after_ingestion,
        )
        df_after_data_structuring = DataStructuringExecutor(
            self.settings,
        ).execute(
            df=df_after_ingestion,
        )
        SemanticRulesExecutor().execute(
            df=df_after_data_structuring,
        )
        logging.info("Pipeline finalizada")

        # logging.info("Validação de regras de negócio iniciada...")
        # RulesValidator(
        #     RuleType.BUSINESS,
        #     [AllowedMinMaxValues(column=col) for col in RANGE_COLUMNS],
        # ).execute(df)
        # RulesValidator(
        #     RuleType.BUSINESS,
        #     [AllowedColumnValues(column=col) for col in LIST_OPTIONS_COLUMNS],
        # ).execute(df)
        # logging.info("Validação de regras de negócio finalizada...\n")
