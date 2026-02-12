from enum import Enum
import polars as pl


class DTypes(Enum):
    Int64 = "Int64"
    Float64 = "Float64"
    String = "String"
    Boolean = "Boolean"
    Date = "Date"

    @classmethod
    def as_dict(cls) -> dict[str, pl.DataType]:
        return {
            cls.Int64.value: pl.Int64(),
            cls.Float64.value: pl.Float64(),
            cls.String.value: pl.String(),
            cls.Boolean.value: pl.Boolean(),
            cls.Date.value: pl.Date(),
        }
