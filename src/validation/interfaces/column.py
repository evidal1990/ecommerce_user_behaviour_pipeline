from typing import Any
from abc import ABC, abstractmethod


class Column(ABC):

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def dtype(self) -> Any:
        pass

    @abstractmethod
    def validate(self, df) -> dict:
        pass
