from typing import Any
import polars as pl
from pathlib import Path
from src.utils import file_io
from src.validation.interfaces.rule import Rule
from consts.validation_status import ValidationStatus


BASE_DIR = Path(__file__).resolve().parents[3]


class ColumnDType(Rule):
    def __init__(self, column: str) -> None:
        self.column = column

    def name(self) -> str:
        return f"dtype_column_{self.column}"

    def dtype(self) -> Any:
        return

    def validate(self, df: pl.DataFrame) -> dict:
        contract = self._load_contract()
        expected = contract["dtypes"][self.column]
        received = df[self.column].dtype
        if expected == received:
            status = ValidationStatus.PASS
        else:
            status = ValidationStatus.FAIL
        return {"status": status, "expected": expected, "received": received}

    def _load_contract(self) -> dict:
        contract_path = BASE_DIR / "src" / "transformation" / "bronze" / "schema.yaml"
        return file_io.read_yaml(contract_path)
