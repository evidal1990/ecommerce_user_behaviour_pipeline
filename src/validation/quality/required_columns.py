import polars as pl
from pathlib import Path
from src.utils import file_io
from src.validation.interfaces.rule import Rule
from consts.validation_status import ValidationStatus


BASE_DIR = Path(__file__).resolve().parents[3]


class RequiredColumns(Rule):
    def __init__(self, column: str, contract: dict) -> None:
        self.column = column
        self._contract = contract

    def name(self) -> str:
        return f"required_columns"

    def validate(self, df: pl.DataFrame) -> dict:
        required = self._contract["columns"][self.column].get("required", False)
        if not required:
            return {}
        return {
            "status": (
                ValidationStatus.PASS
                if self.column in df.columns
                else ValidationStatus.FAIL
            )
        }
