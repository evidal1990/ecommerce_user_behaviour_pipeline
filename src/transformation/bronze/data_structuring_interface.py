from abc import ABC, abstractmethod


class DataStructuringInterface(ABC):

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def execute(self, df) -> dict:
        pass
