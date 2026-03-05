import polars as pl
from src.validation.interfaces.rule import Rule
from consts.validation_status import ValidationStatus
from consts.dtypes import DTypes


DTYPES = DTypes.as_dict()


class ColumnDType(Rule):
    def __init__(self, column: str, contract: dict) -> None:
        self.column = column
        self._contract = contract

    def name(self) -> str:
        return f"{self.column}_DTYPE_RULE"

    def validate(self, df: pl.DataFrame) -> dict:
        dtype_key = self._contract["columns"][self.column].get("dtype")
        if not dtype_key:
            return {
                "status": ValidationStatus.PASS,
                "expected": None,
                "received": None,
            }

        expected = DTYPES[dtype_key]

        if self.column not in df.columns:
            return {
                "status": ValidationStatus.FAIL,
                "expected": expected,
                "received": None,
            }

        received = df[self.column].dtype
        status = (
            ValidationStatus.PASS if expected == received else ValidationStatus.FAIL
        )

        return {
            "status": status,
            "expected": expected,
            "received": received,
        }
