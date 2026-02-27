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
        return f"dtype_column_{self.column}"

    def validate(self, df: pl.DataFrame) -> dict:
        dtype = self._contract["columns"][self.column].get("dtype", False)
        if not dtype:
            return {}
        expected = DTYPES[dtype]
        received = df[self.column].dtype
        if expected == received:
            status = ValidationStatus.PASS
        else:
            status = ValidationStatus.FAIL
        return {"status": status, "expected": expected, "received": received}
