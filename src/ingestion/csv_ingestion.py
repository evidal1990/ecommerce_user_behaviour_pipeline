from pathlib import Path
from src.utils import file_io, df_validation


def load_schema() -> dict:
    return file_io.read_yaml(
        Path(__file__).resolve().parents[1] / "ingestion" / "schema.yaml"
    )


def read_csv(settings):
    return file_io.read_csv(settings)


def validate_required_columns(df, schema):
    df_validation.validate_required_columns(
        df=df, required_columns=schema
    )


def validate_dtypes(df, schema):
    df_validation.validate_dtypes(df=df, dtype_schema=schema)


def write_csv_to_local_raw_layer(df, path:str):
    file_io.write_csv(df=df, path=path)
