from datetime import datetime
import polars as pl
import logging


class SemanticRules:
    def __init__(self, df: pl.DataFrame) -> None:
        self.df = df

    def execute(self) -> None:
        current_date = datetime.now().date()
        duplicated_ids = self._check_duplicated_user_id()
        last_purchase_date_result = self._check_future_dates(
            "last_purchase_date", current_date
        )
        income_level_result = self._check_annual_income()

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
        users = (
            self.df["user_id"]
            .filter(self.df["user_id"].is_duplicated())
            .unique()
            .to_list()
        )
        if users:
            message = f"user_id duplicados encontrados: {users}"
            log_lvl = logging.critical
        else:
            message = "Coluna user_id sem valores duplicados"
            log_lvl = logging.info
        log_lvl(message)
        logging.info("Verificação de duplicidade concluída.")

        return users

    def _check_future_dates(self, column, expected_date) -> list[pl.Date]:
        users = (
            self.df.filter(pl.col(column) > expected_date)
            .select(["user_id", column])
            .to_dict()
        )
        users_total = len(users)
        if users_total > 0:
            message = f"Usuários com {column} maior do que a data esperada: {users}"
            log_lvl = logging.error
        else:
            message = f"Nenhuma inconsistência encontrada para a coluna {column}"
            log_lvl = logging.info
        log_lvl(message)

        return users

    def _check_annual_income(self) -> dict:
        """
        Verifica se há usuários com annual_income negativo.

        Se houver usuários com annual_income negativo, registra um log de error com a lista
        de usuários e seus respectivos valores de annual_income. Caso contrário, registra um log de info com a mensagem
        de que não há usuários com annual_income negativo.

        Retorno:
            dict: Dicionário com os usuários e seus respectivos valores de annual_income.
        """
        EXPECTED_MIN = 0
        users = dict()
        if self.df["annual_income"].min() < EXPECTED_MIN:
            users = (
                self.df.filter(pl.col("annual_income") < EXPECTED_MIN)
                .select(["user_id", "annual_income"])
                .to_dict()
            )
            message = f"Usuários com annual_income < {EXPECTED_MIN}: {users}"
            log_lvl = logging.error
        else:
            message = "Nenhum usuário com annual_income negativo."
            log_lvl = logging.info
        log_lvl(message)
        return users
