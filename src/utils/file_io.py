import yaml
from typing import Any


def read_yaml(path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return data

