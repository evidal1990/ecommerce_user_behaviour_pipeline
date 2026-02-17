import logging
import polars as pl
import kagglehub
from pathlib import Path
from src.utils import file_io
from src.validation.quality_checks import QualityChecks

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
        quality_checks = QualityChecks(self.df, self._contract)
        quality_checks._validate_required_columns()
        quality_checks._validate_dtypes()
        self._write_raw()

    def _read_csv(self) -> pl.DataFrame:
        """
        Realiza a leitura de um arquivo CSV e retorna um objeto
        DataFrame Polars.

        Retorno:
            pl.DataFrame: Dataframe Polars com os dados lidos do arquivo CSV.
        """
        if not self._settings["origin"]:
            raise ValueError("Arquivo de origem não informado.")

        dataset = Path(kagglehub.dataset_download(self._settings["origin"]))
        dataset_list = list(dataset.glob("*.csv"))
        if dataset_list == []:
            raise FileNotFoundError("Dataset baixado não foi encontrado.")
        logging.info(f"CSV disponível em {dataset_list[0]}")

        df = pl.read_csv(dataset_list[0])
        if df.is_empty():
            raise ValueError("Dataframe de origem está vazio.")
        logging.info("Leitura de dados na origem concluída com sucesso.")
        return df

    def _write_raw(self) -> None:
        """
        Realiza a escrita dos dados do DataFrame na camada raw.

        Retorno:
            None
        """
        path = self._settings["destination"]["raw"]
        if not Path(path).parent.exists():
            Path(path).parent.mkdir()
        self.df.write_csv(path)
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
