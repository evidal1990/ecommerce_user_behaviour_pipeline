import logging
import polars as pl
from pathlib import Path
from src.utils import file_io, dataframe

BASE_DIR = Path(__file__).resolve().parents[2]


class CsvIngestion:
    def __init__(self, settings: dict) -> None:
        """
        Inicializa o objeto CsvIngestion com as configura es fornecidas.

        Parametros:
            settings (dict): Configura es do pipeline.

        Retorno:
            None
        """
        self.df = None
        self._settings = settings["data"]
        self._contract = self._load_contract()

    def execute(self) -> None:
        """
        Executa o pipeline de ingestão de CSV.

        Inicia a ingestão de CSV com as configurações fornecidas,
        executa a classe CsvIngestion e registra logs de início e fim
        da execução.

        Retorno:
            None
        """
        self.df = self._read_csv()
        self._validate_required_columns()
        self._validate_dtypes()
        self._write_raw()

    def _read_csv(self) -> pl.DataFrame:
        """
        Realiza a leitura de um arquivo CSV e retorna um objeto
        DataFrame Polars.

        Retorno:
            pl.DataFrame: Dataframe Polars com os dados lidos do arquivo CSV.
        """
        df = pl.read_csv(self._settings["origin"])
        if df.is_empty():
            raise ValueError("Dataframe de origem está vazio.")
        logging.info("Leitura de dados na origem concluída com sucesso.")
        return df

    def _validate_required_columns(self) -> None:
        missing_columns = dataframe.validate_required_columns(
            df=self.df, required_columns=self._contract["required_columns"]
        )
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")
        logging.info("Validação de colunas obrigatórias concluída com sucesso")

    def _validate_dtypes(self) -> None:
        """
        Valida se os tipos de colunas do DataFrame de origem
        convergem com os tipos especificados no contrato.

        Se houver divergências, registra um log de warning com as colunas
        e seus respectivos tipos divergentes.

        Retorno:
            None
        """
        divergences = dataframe.validate_dtypes(
            df=self.df, dtype_schema=self._contract["dtypes"]
        )
        if divergences:
            logging.warning(f"Colunas com tipos divergentes: {divergences}")
        logging.info("Validação de tipos de colunas concluída com sucesso.")

    def _write_raw(self) -> None:
        """
        Realiza a escrita dos dados do DataFrame na camada raw.

        Retorno:
            None
        """
        self.df.write_csv(self._settings["destination"]["raw"])
        logging.info("Escrita de dados na camada raw concluída com sucesso")

    def _load_contract(self) -> dict:
        """
        Carrega o contrato de ingestão de CSV, que é um arquivo YAML
        com as configurações de ingestão de CSV.

        Retorno:
            dict: Contrato de ingestão de CSV.
        """
        contract_path = BASE_DIR / "src" / "ingestion" / "schema.yaml"
        return file_io.read_yaml(contract_path)
