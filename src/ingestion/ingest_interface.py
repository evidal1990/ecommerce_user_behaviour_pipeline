from abc import ABC, abstractmethod


class IngestInterface(ABC):

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def execute(self, df) -> dict:
        pass
