import logging
import polars as pl
from pathlib import Path
from consts.rule_type import RuleType
from src.utils import file_io
from src.validation import RulesValidator
from src.validation.business.allowed_min_max_values import AllowedMinMaxValues


BASE_DIR = Path(__file__).resolve().parents[3]


class BusinessRulesExecutor:
    def __init__(self) -> None:
        pass

    def start(self, df: pl.DataFrame) -> None:
        logging.info("Validação de regras de negócio iniciada")

        contract = self._get_contract()
        contract_columns = contract["columns"].items()
        for key, rules in contract_columns:
            if (
                "business_rules" not in rules
                or "min" not in rules
                or "max" not in rules
                or not rules["business_rules"]
            ):
                continue
            RulesValidator(
                RuleType.BUSINESS,
                [
                    AllowedMinMaxValues(
                        column=key,
                        min=rules["min"],
                        max=rules["max"],
                    )
                ],
            ).execute(df)
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
