import logging
import polars as pl
from pathlib import Path
from consts.rule_type import RuleType
from src.utils import file_io
from src.validation import RulesValidator
from src.validation.business.allowed_min_max_values import AllowedMinMaxValues
from src.validation.business.allowed_column_values import AllowedColumnValues


BASE_DIR = Path(__file__).resolve().parents[3]


class BusinessRulesExecutor:
    def __init__(self) -> None:
        pass

    def start(self, df: pl.DataFrame) -> None:
        logging.info("Validação de regras de negócio iniciada")

        contract = self._get_contract()
        contract_columns = contract["columns"]

        rules = []

        for key, value in contract_columns.items():
            if {"min", "max"}.issubset(value):
                rules.append(
                    AllowedMinMaxValues(
                        column=key,
                        min=value["min"],
                        max=value["max"],
                    )
                )

            if {"values"}.issubset(value):
                rules.append(
                    AllowedColumnValues(
                        column=key,
                        values=value["values"],
                    )
                )

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
