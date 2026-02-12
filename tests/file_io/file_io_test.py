import pytest
from src.utils import file_io


def test_read_valid_yaml() -> None:
    data = file_io.read_yaml("tests/data/test.yaml")
    assert data is not None


def test_read_not_found_yaml() -> None:
    path = "tests/data/invalid.yaml"
    with pytest.raises(FileNotFoundError):
        file_io.read_yaml(path)
