from pathlib import Path
from google.auth import default
from src.utils import get_env_variables, file_io
from storage.gcs import GoogleCloudStorage
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parents[2]
config_path = BASE_DIR / "config" / "google_cloud_platform.yaml"
gcp_configs = file_io.read_yaml(config_path)
environment = get_env_variables.load()
cloud_storage = gcp_configs[environment]["cloud_storage"]
credentials, project = default()
gcs = GoogleCloudStorage(cloud_storage["bucket"])


def test_get_blobs() -> None:
    blobs = gcs.get_blobs()
    assert "raw/" in blobs
    assert "bronze/" in blobs
    assert "silver/" in blobs
    assert "gold/" in blobs


def test_add_file() -> None:
    blob = f'{cloud_storage["paths"]["raw"]}{datetime.now()}'
    file = gcs.add_file(blob=blob, file_path="data/raw/data.test.csv")
    assert file is True
