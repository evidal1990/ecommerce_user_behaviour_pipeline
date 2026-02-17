import polars as pl
import pytest
from datetime import datetime
from src.transformation.bronze.structure_data import StructureData


def test_data_transformation() -> None:
    settings = {
        "data": {
            "bronze": {
                "origin": "tests/data/test.csv",
                "destination": f"tests/results/test_{datetime.now()}.csv",
            }
        }
    }

    StructureData(settings).execute()
    df_destination = pl.read_csv(settings["data"]["bronze"]["destination"])
    assert not df_destination.is_empty()
    assert "is_weekend_shopper" in df_destination.columns
    assert df_destination["income_level"].dtype == pl.Float64


def test_data_transformation_without_settings() -> None:
    with pytest.raises(KeyError, match="data"):
        StructureData({}).execute()
