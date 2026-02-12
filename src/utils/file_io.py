import yaml
from typing import Any


def read_yaml(path) -> Any:
    """
    Lê um arquivo YAML e retorna o seu conteúdo como um objeto Python.

    Parâmetros:
        path (str): Caminho do arquivo YAML a ser lido.

    Retorno:
        Any: Conteúdo do arquivo YAML como um objeto Python.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return data

