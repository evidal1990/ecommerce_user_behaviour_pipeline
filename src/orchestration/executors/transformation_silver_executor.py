import logging
import polars as pl
from pathlib import Path
from src.transformation.silver.enrich.enrich_dataframe import EnrichDataFrame
from src.transformation.silver.clean.clean import CleanData
from src.transformation.silver.normalize.normalize import Normalize


class TransformationSilverExecutor:
    def __init__(self, settings: dict) -> None:
        data = settings.get("data")
        if not data or "silver" not in data:
            raise ValueError("Configuração de silver não encontrada")

        self._settings = data["silver"]

    def start(self, df: pl.DataFrame) -> pl.DataFrame:
        logging.info("Transformação de dados provenientes da camada bronze iniciada")

        df = CleanData().execute(df=df)
        df = Normalize().execute(df=df)
        df = EnrichDataFrame(settings=self._settings).execute(df=df)
        self._write_silver(df=df)
        logging.info(
            "Transformação de dados provenientes da camada bronze finalizada\n"
        )
        return df

    def _write_silver(self, df) -> None:
        path = self._settings["destination"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(path)
