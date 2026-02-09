from google.auth import default
from src.utils import get_env_variables, get_yaml_data
from storage import gcs
from datetime import datetime

credentials, project = default()
environment = get_env_variables.load()
cloud_storage = get_yaml_data.get(environment)["cloud_storage"]
bucket = cloud_storage["bucket"]
paths = cloud_storage["paths"]


def test_get_blobs():
    blobs = gcs.get_blobs(bucket)
    assert "raw/" in blobs
    assert "bronze/" in blobs
    assert "silver/" in blobs
    assert "gold/" in blobs


def test_add_file():
    blob = f'{paths["raw"]}{datetime.now()}'
    file = gcs.add_file(bucket=bucket, blob=blob, file_path="data/raw/file.txt")
    assert file is True

