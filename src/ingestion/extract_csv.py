from pathlib import Path
from src.utils import file_io, df_validation


def _load_settings() -> dict:
    return file_io.read_yaml(
        Path(__file__).resolve().parents[2] / "config" / "settings.yaml"
    )


def _load_schema() -> dict:
    return file_io.read_yaml(
        Path(__file__).resolve().parents[1] / "ingestion" / "schema.yaml"
    )


def init():
    settings, schema = _load_settings(), _load_schema()

    df = file_io.read_csv(settings["origin"]["path"])
    print(df)
    df_validation.validate_required_columns(
        df=df, required_columns=schema["required_columns"]
    )
    df_validation.validate_dtypes(
        df=df, 
        dtype_schema=schema["dtypes"]
    )
    file_io.write_csv(
        df=df, 
        path="data/raw/data.csv"
    )
