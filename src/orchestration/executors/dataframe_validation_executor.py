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

    def start(self, df: pl.DataFrame) -> None:
        logging.info("Validação da estrutura do dataframe iniciada")
        contract = self._get_contract()
        for column in DF_COLUMNS:
            DataFrameValidator(
                RuleType.DATAFRAME_STRUCTURE,
                [
                    RequiredColumns(
                        column=column,
                        contract=contract,
                    )
                ],
            ).execute(df)
            DtypeValidator(
                RuleType.DATAFRAME_STRUCTURE,
                [
                    ColumnDType(
                        column=column,
                        contract=contract,
                    )
                ],
            ).execute(df)
            RulesValidator(
                RuleType.DATAFRAME_STRUCTURE,
                [
                    NotAllowedNullCount(
                        column=column,
                    )
                ],
            ).execute(df)
        logging.info("Validação da estrutura do dataframe finalizada\n")

    def _get_contract(self) -> dict:
        path = BASE_DIR.joinpath(
            "src",
            "validation",
            "quality",
            "schema.yaml",
        )
        try:
            return file_io.read_yaml(path)
        except FileNotFoundError:
            logging.error(f"Schema não encontrado em {path}")
            raise
