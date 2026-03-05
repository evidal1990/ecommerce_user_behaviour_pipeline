import logging
import polars as pl
from pathlib import Path
from consts.rule_type import RuleType
from consts.employment_status import EmploymentStatus
from src.utils import file_io
from src.validation import RulesValidator
from src.validation.business.employment_status_income import IncomePerEmploymentStatus


BASE_DIR = Path(__file__).resolve().parents[3]


class BusinessRulesExecutor:
    def __init__(self) -> None:
        pass

    def start(self, df: pl.DataFrame) -> None:
        logging.info("Validação de regras de negócio iniciada")

        rules = [
            IncomePerEmploymentStatus(status=EmploymentStatus.EMPLOYED),
            IncomePerEmploymentStatus(status=EmploymentStatus.SELF_EMPLOYED),
            IncomePerEmploymentStatus(status=EmploymentStatus.UNEMPLOYED),
        ]

        RulesValidator(RuleType.BUSINESS, rules).execute(df)
        logging.info("Validação de regras de negócio finalizada\n")

    def _get_contract(self) -> dict:
        path = BASE_DIR.joinpath(
            "src",
            "validation",
            "business",
            "schema.yaml",
        )
        try:
            return file_io.read_yaml(path)
        except FileNotFoundError:
            logging.error(f"Schema não encontrado em {path}")
            raise
