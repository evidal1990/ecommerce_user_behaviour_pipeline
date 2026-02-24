from datetime import datetime
import polars as pl
import logging


class SemanticRules:
    def __init__(self, df: pl.DataFrame) -> None:
        self.df = df

    def execute(self) -> None:
        duplicated_ids = self._check_duplicated_user_id()
        last_purchase_date_result = self._check_future_dates("last_purchase_date")

    def _check_duplicated_user_id(self) -> list:
        """
        Verifica se há valores duplicados na coluna user_id.

        Se houver valores duplicados, registra um log de critical com a lista
        de valores duplicados. Caso contrário, registra um log de info com a mensagem
        de que a coluna não tem valores duplicados.

        Retorno:
            None
        """
        logging.info("Verificando duplicidade de user_id...")
        duplicated_ids = (
            self.df["user_id"]
            .filter(self.df["user_id"].is_duplicated())
            .unique()
            .to_list()
        )
        if duplicated_ids:
            message = f"user_id duplicados encontrados: {duplicated_ids}"
            log_lvl = logging.critical
        else:
            message = "Coluna user_id sem valores duplicados"
            log_lvl = logging.info
        log_lvl(message)
        logging.info("Verificação de duplicidade concluída.")

        return duplicated_ids

    def _check_future_dates(self, column) -> list[pl.Date]:
        current_date = datetime.now().date()
        future_dates = [row for row in self.df[column].to_list() if row > current_date]
        dates_greather_than_now = len(future_dates)
        if dates_greather_than_now > 0:
            message = f"Coluna {column}: {dates_greather_than_now} datas maiores que a data atual"
            log_lvl = logging.error
        else:
            message = f"Coluna {column}: Nenhuma data maior que a data atual foi encontrada"
            log_lvl = logging.info
        log_lvl(message)

        return future_dates
