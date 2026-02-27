import logging
from consts.rule_type import RuleType
from src.orchestration import (
    DF_COLUMNS,
    NOT_ALLOWED_NULL_COLUMNS,
    RANGE_COLUMNS,
    LIST_OPTIONS_COLUMNS,
    SEMANTIC_MIN_VALUE_COLUMNS,
    DATE_COLUMNS,
)
from src.orchestration.pipeline_steps import PipelineSteps
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
        PipelineSteps(
            settings=self.settings
        ).execute_csv_ingestion().execute_dataframe_structure_validation()

        # logging.info("Estruturação de dados brutos iniciada...")
        # df = StructureData(self.settings).execute()
        # logging.info("Estruturação de dados brutos finalizada...\n")

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
