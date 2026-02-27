import logging
import polars as pl
from .ingest_interface import IngestInterface
from consts.ingestion_status import IngestionStatus


class Ingest:
    def __init__(self, ingestions: list[IngestInterface]) -> None:
        self.ingestions = ingestions

    def execute(self) -> pl.DataFrame:
        results = {}
        for ingestion in self.ingestions:
            result = ingestion.execute()
            results[ingestion.name()] = result
            status = result["status"]
            message = (
                f"[{ingestion.name()}_INGESTION]\n"
                f"dataset_found={result["dataset_found"]}\n"
                f"from={result["from"]}\n"
                f"to={result["to"]}\n"
            )
            if not result["dataset_found"]:
                raise ValueError("Dataframe de origem está vazio.")
            log_lvl = (
                logging.info
                if status == IngestionStatus.PASS
                else (
                    logging.warning if status == IngestionStatus.WARN else logging.error
                )
            )
            log_lvl(message)
        return result["dataframe"]
