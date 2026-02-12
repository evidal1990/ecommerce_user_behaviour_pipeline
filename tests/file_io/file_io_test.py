import pytest
from src.utils import file_io


def test_read_valid_yaml() -> None:
    """
    Testa se o arquivo tests/data/test.yaml pode ser lido com sucesso.
    O arquivo contém informações de teste e deve ser lido sem erros.
    """
    data = file_io.read_yaml("tests/data/test.yaml")
    assert data is not None


def test_read_not_found_yaml() -> None:
    """
    Testa se o arquivo "tests/data/invalid.yaml" não pode ser lido com sucesso.
    O arquivo não existe e deve ser levantado um erro FileNotFoundError.
    """
    path = "tests/data/invalid.yaml"
    with pytest.raises(FileNotFoundError):
        file_io.read_yaml(path)
