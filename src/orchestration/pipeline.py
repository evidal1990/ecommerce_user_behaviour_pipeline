import logging
from src.orchestration.executors.ingestion_executor import IngestionExecutor
from src.orchestration.executors.dataframe_validation_executor import (
    DataFrameValidatorExecutor,
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
        df_after_ingestion = IngestionExecutor(settings=self.settings).execute()
        DataFrameValidatorExecutor().execute(df_after_ingestion)
        logging.info("Pipeline finalizada")

        # logging.info("Validação de regras semânticas iniciada...")
        # RulesValidator(
        #     RuleType.SEMANTIC,
        #     [
        #         MinValue(column=key, min_limit=value)
        #         for key, value in SEMANTIC_MIN_VALUE_COLUMNS.items()
        #     ],
        # ).execute(df)
        # RulesValidator(
        #     RuleType.SEMANTIC,
        #     [DuplicatedUserId()],
        # ).execute(df)
        # RulesValidator(
        #     RuleType.SEMANTIC,
        #     [
        #         FutureDates(column=key, date_limit=value)
        #         for key, value in DATE_COLUMNS.items()
        #     ],
        # ).execute(df)
        # RulesValidator(
        #     RuleType.SEMANTIC,
        #     [
        #         EmployedWithoutIncome(),
        #         SelfEmployedWithoutIncome(),
        #         UnemployedUserWithIncome(),
        #     ],
        # ).execute(df)
        # logging.info("Validação de regras semânticas finalizada...\n")

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
