from abc import ABC, abstractmethod


class SemanticRule(ABC):

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def validate(self, df) -> dict:
        pass
