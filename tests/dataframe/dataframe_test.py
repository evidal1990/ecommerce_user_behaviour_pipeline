import pytest
import polars as pl
from datetime import date
from src.utils import dataframe
from consts.dtypes import DTypes


DTYPE_SCHEMA = {
    "col1": DTypes.Int64.value,
    "col2": DTypes.Float64.value,
    "col3": DTypes.String.value,
    "col4": DTypes.Boolean.value,
    "col5": DTypes.Date.value,
}

REQUIRED_COLUMNS = {"col1", "col2", "col3","col4", "col5"}

def test_df_required_columns_without_divergence() -> None:
    df = pl.DataFrame(
        {
            "col1": 10, 
            "col2": 2.0, 
            "col3": "texto", 
            "col4": True, 
            "col5": date.today()
        }
    )
    result = dataframe.validate_required_columns(
        df=df, required_columns=REQUIRED_COLUMNS
    )
    assert result == []


def test_df_required_columns_with_divergence() -> None:
    df = pl.DataFrame(
        {
            "col1": 10, 
            "col2": 2.0, 
            "col3": "texto"
        }
    )
    result = dataframe.validate_required_columns(
        df=df, required_columns=REQUIRED_COLUMNS
    )
    assert "col4" and "col5" in result

def test_dtypes_without_divergence() -> None:
    df = pl.DataFrame(
        {
            "col1": 10, 
            "col2": 2.0, 
            "col3": "texto", 
            "col4": True, 
            "col5": date.today()
        }
    )

    result = dataframe.validate_dtypes(df=df, dtype_schema=DTYPE_SCHEMA)
    assert result == {}


def test_dtypes_with_divergence() -> None:
    df = pl.DataFrame(
        {
            "col1": "texto", 
            "col2": 10, 
            "col3": True, 
            "col4": date.today(), 
            "col5": 2.0
        }
    )

    result = dataframe.validate_dtypes(df=df, dtype_schema=DTYPE_SCHEMA)
    assert result == {
        "col1": {"expected": pl.Int64(), "received": pl.String()},
        "col2": {"expected": pl.Float64(), "received": pl.Int64()},
        "col3": {"expected": pl.String(), "received": pl.Boolean()},
        "col4": {"expected": pl.Boolean(), "received": pl.Date()},
        "col5": {"expected": pl.Date(), "received": pl.Float64()},
    }

def test_invalid_dtypes() -> None:
    dtype = pl.Int32()
    invalid_dtype_schema = {"col1": dtype}
    df = pl.DataFrame({"col1": 10,})

    with pytest.raises(TypeError, match=f"Tipo {dtype} não está mapeado"):
        dataframe.validate_dtypes(df=df, dtype_schema=invalid_dtype_schema)


def test_not_found_df_column() -> None:
    df = pl.DataFrame(
        {
            "col1": 10, 
            "col2": 2.0, 
            "col4": True, 
            "col5": date.today(),
        }
    )

    with pytest.raises(ValueError, match=f"Coluna col3 não está mapeada no schema"):
        dataframe.validate_dtypes(df=df, dtype_schema=DTYPE_SCHEMA)
