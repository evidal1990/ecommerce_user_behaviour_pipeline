import polars as pl
from src.transformation.bronze.structure_data import StructureData

def test_data_transformation() -> None:
    settings = {
        "data": {
            "origin": "dhrubangtalukdar/e-commerce-shopper-behavior-amazonshopify-based",
            "destination": {"raw": f"tests/results/test_{datetime.now()}.csv"},
        }
    }