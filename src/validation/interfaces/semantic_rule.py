import polars as pl
from abc import ABC, abstractmethod
from consts.validation_status import ValidationStatus


class SemanticRule(ABC):

    def __init__(self, sample_size: int) -> None:
        self.sample_size = sample_size
        self._total_records = 0
        self._invalid_records = 0
        self._invalid_percentage = 0.0
        self._sample = []
        self._status = ValidationStatus.FAIL

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def invalid_df(self, df: pl.DataFrame) -> pl.DataFrame:
        pass

    @abstractmethod
    def decide_status(self) -> ValidationStatus:
        pass

    def validate(self, df) -> dict:
        self._total_records = self.total_records(df=df)
        invalid_df = self.invalid_df(df=df)
        self._invalid_records = invalid_df.height
        self._invalid_percentage = self.percentage()
        self._sample = self.build_sample(df=invalid_df)
        self._status = self.decide_status()

        return self.result()

    def total_records(self, df: pl.DataFrame) -> int:
        return df.height

    def build_sample(self, df: pl.DataFrame) -> list:
        if df.is_empty():
            return []
        return self.sample(df, self.sample_column())

    def sample_column(self) -> str:
        raise NotImplementedError

    def percentage(self) -> float:
        if self._total_records == 0:
            return 0.0
        return round(
            (self._invalid_records / self._total_records) * 100,
            2,
        )

    def sample(self, df: pl.DataFrame, column: str) -> list:
        return df.select(column).head(self.sample_size).to_series().to_list()

    def result(self) -> dict:
        return {
            "status": self._status,
            "total_records": self._total_records,
            "invalid_records": self._invalid_records,
            "invalid_percentage": self._invalid_percentage,
            "sample": self._sample,
        }
