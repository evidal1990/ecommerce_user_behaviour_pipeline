import logging
from typing import Self
from pathlib import Path
from consts.rule_type import RuleType
from src.utils import file_io
from src.orchestration import DF_COLUMNS
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

BASE_DIR = Path(__file__).resolve().parents[2]


class PipelineSteps:
    def __init__(self, settings) -> None:
        self.df = None
        self.settings = settings
        self.contract = None

    def _load_contract(self, path: str) -> dict:
        return file_io.read_yaml(path)

    def execute_dataframe_structure_validation(self) -> Self:
        logging.info("Validação da estrutura do dataframe iniciada")
        self.contract = self._load_contract(
            BASE_DIR / "src" / "validation" / "quality" / "schema.yaml"
        )
        DataFrameValidator(
            RuleType.DATAFRAME_STRUCTURE,
            [RequiredColumns(column=col, contract=self.contract) for col in DF_COLUMNS],
        ).execute(self.df)
        DtypeValidator(
            RuleType.DATAFRAME_STRUCTURE,
            [ColumnDType(column=col, contract=self.contract) for col in DF_COLUMNS],
        ).execute(self.df)
        RulesValidator(
            RuleType.DATAFRAME_STRUCTURE,
            [NotAllowedNullCount(column=col) for col in DF_COLUMNS],
        ).execute(self.df)
        logging.info("Validação da estrutura do dataframe finalizada\n")
        return self

    def execute_data_structuring(self) -> Self:
        logging.info("Estruturação de dados brutos iniciada")
        self.df = StructureData(self.settings).execute()
        logging.info("Estruturação de dados brutos finalizada\n")

    def execute_semantic_validation(self) -> Self:
        logging.info("Validação das regras semânticas iniciada")
        self.contract = self._load_contract(
            BASE_DIR / "src" / "validation" / "semantic" / "schema.yaml"
        )
        logging.info("Validação das regras semânticas finalizada")
