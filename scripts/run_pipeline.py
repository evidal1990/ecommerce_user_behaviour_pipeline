from pathlib import Path
from src.ingestion import csv_ingestion
from src.utils import file_io


def execute():
    settings = file_io.read_yaml(
        Path(__file__).resolve().parents[1] / "config" / "settings.yaml"
    )
    ingestion_contract = csv_ingestion.load_schema()
    df_origin = csv_ingestion.read_csv(settings["data"]["origin"])
    csv_ingestion.validate_required_columns(
        df_origin, ingestion_contract["required_columns"]
    )
    csv_ingestion.validate_dtypes(df_origin, ingestion_contract["dtypes"])
    csv_ingestion.write_csv_to_local_raw_layer(
        df_origin, settings["data"]["destination"]["raw"]
    )


execute()
