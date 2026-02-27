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
        path = self._settings["destination"]
        if not Path(path).parent.exists():
            Path(path).parent.mkdir()
        self.df.write_csv(path)
        if not df_is_empty:
            status = IngestionStatus.PASS
        else:
            status = IngestionStatus.FAIL
        return {
            "status": status,
            "dataset_found": not df_is_empty,
            "dataframe": self.df,
            "from": self.origin,
            "to": path,
        }
