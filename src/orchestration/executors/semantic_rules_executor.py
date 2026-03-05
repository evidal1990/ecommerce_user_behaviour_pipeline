import logging
from pathlib import Path
import polars as pl
from datetime import datetime
from consts.rule_type import RuleType
from src.utils import file_io
from src.validation import RulesValidator
from src.validation.semantic.duplicated_user_id import DuplicatedUserId
from src.validation.semantic.future_dates import FutureDates
from src.validation.semantic.allowed_min_value import AllowedMinValue
from src.validation.semantic.allowed_max_value import AllowedMaxValue
from src.validation.semantic.allowed_column_values import AllowedColumnValues
from src.validation.semantic.allowed_null_columns import AllowedNullCount


BASE_DIR = Path(__file__).resolve().parents[3]


class SemanticRulesExecutor:
    def __init__(self) -> None:
        pass

    def start(self, df: pl.DataFrame) -> None:
        logging.info("Validação semântica do dataframe iniciada")
        contract = self._get_contract()
        rules = []
        rules.append(DuplicatedUserId())
        rules.append(
            FutureDates(
                column="last_purchase_date",
                date_limit=datetime.now().date(),
            )
        )
        for column, config in contract["columns"].items():
            if "not_null" in config and config["not_null"]:
                rules.append(AllowedNullCount(column=column))
            if "min" in config:
                rules.append(
                    AllowedMinValue(
                        column=column,
                        min=config["min"],
                    )
                )
            if "max" in config:
                rules.append(
                    AllowedMaxValue(
                        column=column,
                        max=config["max"],
                    )
                )
            if "values" in config:
                rules.append(
                    AllowedColumnValues(
                        column=column,
                        values=config["values"],
                    )
                )

        RulesValidator(RuleType.SEMANTIC, rules).execute(df)
        logging.info("Validação semântica do dataframe finalizada\n")

    def _get_contract(self) -> dict:
        path = BASE_DIR.joinpath(
            "src",
            "validation",
            "semantic",
            "schema.yaml",
        )
        try:
            return file_io.read_yaml(path)
        except FileNotFoundError:
            logging.error(f"Schema não encontrado em {path}")
            raise
