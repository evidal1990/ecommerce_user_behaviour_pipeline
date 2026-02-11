from pathlib import Path
from google.auth import default
from src.utils import get_env_variables, file_io

credentials, project = default()


def test_exist_credentials():
    assert credentials is not None, "As credenciais do GCP não foram carregadas."


def test_project_name() -> None:
    environment = get_env_variables.load()
    BASE_DIR = Path(__file__).resolve().parents[2]
    config_path = BASE_DIR / "config" / "google_cloud_platform.yaml"
    gcp_configs = file_io.read_yaml(config_path)
    assert (
        project == gcp_configs[environment]["gcp"]["project_id"]
    ), "O ID do projeto não corresponde ao esperado."
