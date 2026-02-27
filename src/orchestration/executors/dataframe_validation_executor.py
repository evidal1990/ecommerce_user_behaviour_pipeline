import logging
import polars as pl
from pathlib import Path
from consts.rule_type import RuleType
from src.orchestration import DF_COLUMNS
from src.utils import file_io
from src.validation import (
    RulesValidator,
    DtypeValidator,
    DataFrameValidator,
)
from src.validation.quality import (
    NotAllowedNullCount,
    RequiredColumns,
    ColumnDType,
)

BASE_DIR = Path(__file__).resolve().parents[3]


class DataFrameValidatorExecutor:
    def __init__(self) -> None:
        pass

    def execute(self, df: pl.DataFrame) -> None:
        logging.info("Validação da estrutura do dataframe iniciada")
        self.contract = file_io.read_yaml(
            BASE_DIR / "src" / "validation" / "quality" / "schema.yaml"
        )
        DataFrameValidator(
            RuleType.DATAFRAME_STRUCTURE,
            [
                RequiredColumns(column=column, contract=self.contract)
                for column in DF_COLUMNS
            ],
        ).execute(df)
        DtypeValidator(
            RuleType.DATAFRAME_STRUCTURE,
            [
                ColumnDType(column=column, contract=self.contract)
                for column in DF_COLUMNS
            ],
        ).execute(df)
        RulesValidator(
            RuleType.DATAFRAME_STRUCTURE,
            [NotAllowedNullCount(column=column) for column in DF_COLUMNS],
        ).execute(df)
        logging.info("Validação da estrutura do dataframe finalizada\n")
