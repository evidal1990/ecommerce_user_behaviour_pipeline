import logging
from typing import Self
import polars as pl
from pathlib import Path
from .clean_executor import CleanExecutor
from .normalize_executor import NormalizeExecutor
from .enrich_executor import EnrichExecutor


class TransformationSilverExecutor:
    def __init__(self, settings: dict) -> None:
        data = settings.get("data")
        if not data or "silver" not in data:
            raise ValueError("Configuração de silver não encontrada")

        self._settings = data["silver"]
        self.df = None

    def start(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        logging.info("Transformação de dados provenientes da camada bronze iniciada")
        self.df = df
        self._clean()._normalize()._enrich()._write_silver()
        logging.info("Transformação de dados provenientes da camada bronze finalizada")
        return self.df

    def _write_silver(
        self
    ) -> None:
        path = self._settings["destination"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.df.write_parquet(path, compression="zstd", statistics=True)

    def _clean(
        self
    ) -> Self:
        df = self.df
        self.df = CleanExecutor().start(df=df)
        return self

    def _normalize(
        self
    ) -> Self:
        df = self.df
        self.df = NormalizeExecutor().start(df=df)
        return self

    def _enrich(
        self
   ) -> Self:
        df = self.df
        self.df = EnrichExecutor(self._settings["parent"]).start(df=df)
        return self
