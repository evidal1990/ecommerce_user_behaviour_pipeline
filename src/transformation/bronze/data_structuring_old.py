import logging
from typing import Self
import polars as pl
from pathlib import Path
from src.utils import file_io
from consts.dtypes import DTypes
from consts.action_status import ActionStatus
from src.transformation.bronze.fix_columns_dtypes import FixColumnsDTypes

DTYPES = DTypes.as_dict()

BASE_DIR = Path(__file__).resolve().parents[3]


class DataStructuring:
    def __init__(self, settings, contract: dict) -> None:
        """
        Inicializa o objeto StructureData com as configurações fornecidas.

        Parametros:
            settings (dict): Configura es do pipeline.

        Retorno:
            None
        """
        self._settings = settings["data"]["bronze"]

    def execute(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Executa o pipeline de estruturação de dados brutos.

        - Leitura de dados na raw, validando o tipo de dados, renomeando colunas e
        escrevendo os dados na camada bronze.

        Retorno:
            pl.DataFrame: Dataframe com os dados estruturados na camada bronze.
        """
        self.contract = file_io.read_yaml(
            BASE_DIR / "src" / "validation" / "quality" / "schema.yaml"
        )
        fix_columns_dtypes_result = FixColumnsDTypes(self.contract).execute(df)
        log_lvl = (
            logging.info
            if status == ActionStatus.PASS
            else (logging.warning if status == ActionStatus.WARN else logging.error)
        )
        log_lvl(fix_columns_dtypes_result)

        return self.df

    def _read_csv(self) -> pl.DataFrame:
        """
        Realiza a leitura de um arquivo CSV na camada raw e retorna um objeto
        DataFrame Polars.

        Retorno:
            pl.DataFrame: Dataframe Polars com os dados lidos do arquivo CSV.
        """
        logging.info("Leitura de dados da camada raw iniciada")
        df = pl.read_csv(self._settings["origin"])
        if df.is_empty():
            raise ValueError("Dataframe de raw está vazio.")
        logging.info("Leitura de dados na raw concluída com sucesso.")
        return df

    def _structure_data(self) -> Self:
        """
        Altera os tipos de dados do DataFrame com base nas configurações do contrato.

        Parametros:
            columns (dict): Dicionário com as configurações de tipo de dados.

        Retorno:
            None
        """
        required = self._contract["columns"][self.column].get("required", False)
        if not required:
            return {}
        dtypes = self._contract["dtypes"]

        for key, value in dtypes.items():
            self.df = self.df.with_columns(pl.col(key).cast(DTYPES[value]))
        logging.info("Tipos de dados alterados com sucesso.")
        return self

    def _rename_columns(self) -> None:
        """
        Renomeia as colunas do DataFrame com base nas configurações do contrato.

        Retorno:
            None
        """
        for key, value in self._contract["from-to"].items():
            self.df = self.df.rename({key: value})
        logging.info(f"Colunas renomeadas com sucesso: {self.df.columns}")
        return self

    def _write_bronze(self) -> None:
        """
        Realiza a escrita dos dados do DataFrame na camada bronze.

        Retorno:
            None
        """
        path = self._settings["destination"]
        if not Path(path).parent.exists():
            Path(path).parent.mkdir()
        self.df.write_csv(path)
        logging.info("Escrita de dados na camada bronze concluída com sucesso")
        return self
