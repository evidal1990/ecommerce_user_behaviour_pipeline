import polars as pl
from pathlib import Path
from src.utils import file_io, df_validation

BASE_DIR = Path(__file__).resolve().parents[2]


def _read_csv(settings) -> pl.DataFrame:
    return file_io.read_csv(settings)


def _validate_required_columns(df, schema) -> dict:
    return df_validation.validate_required_columns(df=df, required_columns=schema)


def _validate_dtypes(df, schema) -> dict:
    return df_validation.validate_dtypes(df=df, dtype_schema=schema)


def _write_csv_to_local_raw_layer(df, path: str):
    file_io.write_csv(df=df, path=path)


def _load_ingestion_contract() -> dict:
    return file_io.read_yaml(BASE_DIR / "src" / "ingestion" / "schema.yaml")


def execute(settings) -> bool:
    contract = _load_ingestion_contract()
    df = _read_csv(settings["data"]["origin"])

    required_columns_result = _validate_required_columns(
        df, contract["required_columns"]
    )
    print(f"Colunas obrigatórias ausentes: {required_columns_result}")
    dtypes_result = _validate_dtypes(df, contract["dtypes"])
    print(f"Colunas com divergência no CSV: {dtypes_result}")
    raw_data_path = settings["data"]["destination"]["raw"]
    _write_csv_to_local_raw_layer(df, raw_data_path)
    return True if Path(BASE_DIR / raw_data_path).exists else False
