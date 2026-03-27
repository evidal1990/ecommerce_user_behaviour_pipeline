import logging
import polars as pl
from pathlib import Path
from src.transformation.gold.aggregate.aggregate_data import AggregateData
from src.transformation.gold.metrics.create_metrics import CreateMetrics


class TransformationGoldExecutor:

    def __init__(
        self,
        settings: dict,
    ) -> None:
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
        self.df = AggregateData().execute(df)
        self._write_gold_aggregations()
        self.df = CreateMetrics().execute(self.df)
        self._write_gold_kpis()
        logging.info("Transformação de dados provenientes da camada silver finalizada")
        return self.df

    def _write_gold_aggregations(self) -> None:
        path = self._settings["destination"]["aggregations"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.df.write_csv(path)
        #self.df.write_parquet(path, compression="zstd", statistics=True)

    def _write_gold_kpis(self) -> None:
        path = self._settings["destination"]["kpis"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.df.write_csv(path)
        #self.df.write_parquet(path, compression="zstd", statistics=True)
