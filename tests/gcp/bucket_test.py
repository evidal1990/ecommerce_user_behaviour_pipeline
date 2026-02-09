from google.auth import default
from src.utils import get_env_variables, get_yaml_data
from storage import gcs

credentials, project = default()
environment = get_env_variables.load()
yaml_data = get_yaml_data.get(environment)
bucket_name = yaml_data["cloud_storage"]["bucket"]


def test_main_folders():
    blobs = gcs.get_blobs(yaml_data["cloud_storage"]["bucket"])
    assert "raw/" in blobs
    assert "bronze/" in blobs
    assert "silver/" in blobs
    assert "gold/" in blobs
