import polars as pl
from pathlib import Path
from consts.data_origin_type import DataOriginType
from consts.ingestion_status import IngestionStatus
from .ingest_interface import IngestInterface


class CsvIngestion(IngestInterface):
    def __init__(self, settings: dict, origin: str) -> None:
        """
        Inicializa o objeto CsvIngestion com as configura es fornecidas.

        Parametros:
            settings (dict): Configura es do pipeline.

        Retorno:
            None
        """
        self.df = None
        self._settings = settings
        self.origin = origin

    def name(self) -> str:
        return DataOriginType.CSV

    def execute(self) -> None:
        """
        Executa o pipeline de ingestão de CSV.

        Inicia a ingestão de CSV com as configurações fornecidas,
        executa a classe CsvIngestion e registra logs de início e fim
        da execução.

        Retorno:
            None
        """
        self.df = pl.read_csv(self.origin)
        df_is_empty = self.df.is_empty()
        if df_is_empty:
            status = IngestionStatus.FAIL
            dataset_found = False
        else:
            status = IngestionStatus.PASS
            dataset_found = True
        path = self._settings["destination"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.df.write_csv(path)
        return {
            "status": status,
            "dataset_found": dataset_found,
            "dataframe": self.df,
            "from": self.origin,
            "to": path,
        }
