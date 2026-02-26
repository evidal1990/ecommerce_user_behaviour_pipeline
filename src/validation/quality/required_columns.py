from typing import Any
import polars as pl
from pathlib import Path
from src.utils import file_io, statistics
from src.validation.interfaces.rule import Rule
from consts.validation_status import ValidationStatus


BASE_DIR = Path(__file__).resolve().parents[3]


class RequiredColumns(Rule):
    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return f"required_columns"

    def dtype(self) -> Any:
        return

    def validate(self, df: pl.DataFrame) -> dict:
        total_columns = len(df.columns)
        contract = self._load_contract()
        columns = [col for col in contract["required_columns"] if col not in df.schema]
        not_found_columns = len(columns)
        if len(columns) == 0:
            status = ValidationStatus.PASS
            percentage = 0.0
        else:
            status = ValidationStatus.FAIL
            percentage = statistics.get_percentage(
                dividend=not_found_columns, divider=total_columns
            )
        return {
            "status": status,
            "total_columns": total_columns,
            "not_found_columns": not_found_columns,
            "not_found_percentage": percentage,
            "columns": columns,
        }

    def _load_contract(self) -> dict:
        contract_path = BASE_DIR / "src" / "transformation" / "bronze" / "schema.yaml"
        return file_io.read_yaml(contract_path)
