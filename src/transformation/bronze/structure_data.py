import logging
import polars as pl
from pathlib import Path
from src.utils import file_io
from consts.dtypes import DTypes

DTYPES = DTypes.as_dict()

BASE_DIR = Path(__file__).resolve().parents[3]


class StructureData:
    def __init__(self, settings) -> None:
        """
        Inicializa o objeto StructureData com as configurações fornecidas.

        Parametros:
            settings (dict): Configura es do pipeline.

        Retorno:
            None
        """
        self.df = None
        self._settings = settings["data"]["bronze"]
        self._contract = self._load_contract()

    def execute(self) -> pl.DataFrame:
        """
        Executa o pipeline de estruturação de dados brutos.

        - Leitura de dados na raw, validando o tipo de dados, renomeando colunas e
        escrevendo os dados na camada bronze.

        Retorno:
            pl.DataFrame: Dataframe com os dados estruturados na camada bronze.
        """
        self.df = self._read_csv()
        self._structure_data()
        self._rename_columns()
        self._write_bronze()

        return self.df

    def _load_contract(self) -> dict:
        """
        Carrega o contrato de estrutura de dados brutos à partir de um arquivo YAML
        com as configurações de estrutura de dados brutos.

        Retorno:
            dict: Contrato de estrutura o de dados brutos.
        """
        contract_path = BASE_DIR / "src" / "transformation" / "bronze" / "schema.yaml"
        return file_io.read_yaml(contract_path)

    def _read_csv(self) -> pl.DataFrame:
        """
        Realiza a leitura de um arquivo CSV na camada raw e retorna um objeto
        DataFrame Polars.

        Retorno:
            pl.DataFrame: Dataframe Polars com os dados lidos do arquivo CSV.
        """
        df = pl.read_csv(self._settings["origin"])
        if df.is_empty():
            raise ValueError("Dataframe de raw está vazio.")
        logging.info("Leitura de dados na raw concluída com sucesso.")
        return df

    def _structure_data(self) -> None:
        """
        Altera os tipos de dados do DataFrame com base nas configurações do contrato.

        Parametros:
            columns (dict): Dicionário com as configurações de tipo de dados.

        Retorno:
            None
        """
        dtypes = self._contract["dtypes"]

        for key, value in dtypes.items():
            self.df = self.df.with_columns(pl.col(key).cast(DTYPES[value]))
        logging.info("Tipos de dados alterados com sucesso.")

    def _rename_columns(self) -> None:
        """
        Renomeia as colunas do DataFrame com base nas configurações do contrato.

        Retorno:
            None
        """
        for key, value in self._contract["from-to"].items():
            self.df = self.df.rename({key: value})
        logging.info(f"Colunas renomeadas com sucesso: {self.df.columns}")

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
