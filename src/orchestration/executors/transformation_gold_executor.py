import logging
import polars as pl
from pathlib import Path


class TransformationGoldExecutor:

    def __init__(self, settings: dict) -> None:
        data = settings.get("data")
        if not data or "gold" not in data:
            raise ValueError("Configuração de gold não encontrada")

        self._settings = data["gold"]
        self.df = None

    def start(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        logging.info("Transformação de dados provenientes da camada silver iniciada")
        self.df = df
        logging.info("Transformação de dados provenientes da camada silver finalizada")
        return self.df

    def _write_gold(self) -> None:
        path = self._settings["destination"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.df.write_parquet(path, compression="zstd", statistics=True)
