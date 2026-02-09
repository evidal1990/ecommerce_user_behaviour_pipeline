from google.auth import default
from src.utils import get_env_variables, get_yaml_data

credentials, project = default()


def test_exist_credentials():
    assert credentials is not None, "As credenciais do GCP não foram carregadas."


def test_project_name():
    environment = get_env_variables.load()
    gcp_configs = get_yaml_data.get(environment)
    assert (
        project == gcp_configs["gcp"]["project_id"]
    ), "O ID do projeto não corresponde ao esperado."
