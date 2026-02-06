from pathlib import Path
import yaml


def get(environment):
    BASE_DIR = Path(__file__).resolve().parents[2]
    config_path = BASE_DIR / "config" / "google_cloud_platform.yaml"
    if not config_path.exists():
        raise FileNotFoundError(
            f"Arquivo de configuração não encontrado: {config_path}"
        )

    with open(config_path, "r") as f:
        yaml_data = yaml.safe_load(f)
        if environment in yaml_data:
            return yaml_data[environment]
        raise KeyError(
            f"Ambiente '{environment}' não encontrado no arquivo de configuração."
        )
