import polars as pl
import logging
from src.utils import dataframe


class QualityChecks:
    def __init__(self, df: pl.DataFrame, contract: dict) -> None:
        """
        Inicializa o objeto QualityChecks com o DataFrame e o contrato fornecidos.

        Parametros:
            df (pl.DataFrame): DataFrame a ser validado.
            contract (dict): Contrato de ingestão de CSV.

        Retorno:
            None
        """
        self.df = df
        self.contract = contract

    def _validate_required_columns(self) -> None:
        """
        Valida se as colunas obrigatórias est o presentes no DataFrame.

        Se houver divergências, registra um log de warning com as colunas
        e seus respectivos tipos divergentes.

        Retorno:
            None
        """
        missing_columns = dataframe.validate_required_columns(
            df=self.df, required_columns=self.contract["required_columns"]
        )
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")
        logging.info("Validação de colunas obrigatórias concluída com sucesso")

    def _validate_dtypes(self) -> dict:
        """
        Valida se os tipos de colunas do DataFrame
        convergem com os tipos especificados no contrato.

        Se houver divergências, registra um log de warning com as colunas
        e seus respectivos tipos divergentes.

        Retorno:
            None
        """
        divergences = dataframe.validate_dtypes(
            df=self.df, dtype_schema=self.contract["dtypes"]
        )
        if divergences:
            logging.warning(f"Colunas com tipos divergentes: {divergences}")
        logging.info("Validação de tipos de colunas concluída com sucesso.")

        return divergences

    def _validate_date_format(self) -> list:
        date_columns = [col for col in self.df if col.dtype == pl.Date]
        for column in date_columns:
            logging.info(f"Coluna {column}")
            invalid_formats = self.df.with_columns(
                pl.col(column)
                .str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
                .is_not_null()
            )
            if invalid_formats:
                logging.error(
                    f"Total de datas com formato inválido: {len(invalid_formats)}"
                )
            return invalid_formats
