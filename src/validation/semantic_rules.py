from datetime import datetime
from consts.error_code import ErrorCode
from consts.validation_status import ValidationStatus
import polars as pl
import logging


class SemanticRules:
    def __init__(self, df: pl.DataFrame) -> None:
        self.df = df
        self.TOTAL_RECORDS = self.df.shape[0]

    def execute(self) -> dict:
        result = {}
        current_date = datetime.now().date()
        result.update(
            {
                "duplicated_user_id": self._check_duplicated_user_id(),
                "future_last_purchase_date": self._check_future_dates(
                    "last_purchase_date", current_date
                ),
            }
        )
        # min_rules = self._check_min_rules([
        #     "annual_income",
        #     "household_size",
        #     "monthly_spend",
        #     "average_order_value",
        #     "daily_session_time_minutes",
        #     "cart_items_average",
        #     "account_age_months",
        # ])
        # employment_status = self._check_employment_status_and_annual_income()

        # return {
        #     "duplicated_user_ids": duplicated_user_ids,
        #     "future_dates": future_dates,
        #     "min_rules": min_rules,
        #     "employment_status": employment_status,
        # }
        logging.info(result)

    def _check_duplicated_user_id(self) -> list:
        """
        Verifica se a coluna user_id do DataFrame tem valores duplicados.

        Se houver duplicidade, registra um log de warning com a lista de user_id duplicados.
        Caso contrário, registra um log de info com mensagem de que a coluna não tem valores duplicados.

        Retorno:
            list: Lista de user_id duplicados encontrados.
        """
        logging.info("Verificando duplicidade de user_id...")
        users = (
            self.df["user_id"]
            .filter(self.df["user_id"].is_duplicated())
            .unique()
            .to_list()
        )
        if users:
            status = ValidationStatus.FAIL
            error_code = ErrorCode.DUPLICATED_USER_ID
            message = f"user_id duplicados encontrados: {users}"
            log_lvl = logging.critical
        else:
            status = ValidationStatus.PASS
            error_code = None
            message = "Coluna user_id sem valores duplicados"
            log_lvl = logging.info
        log_lvl(message)
        logging.info("Verificação de duplicidade concluída.")

        return {
            "status": status,
            "error_code": error_code,
            "total_records": self.TOTAL_RECORDS,
            "invalid_records": len(users),
            "invalid_percentage": len(users) / self.TOTAL_RECORDS,
            "users": users,
        }

    def _check_future_dates(self, column, expected_date) -> pl.DataFrame:
        """
        Verifica se a coluna do DataFrame tem valores maior do que a data esperada.

        Se houver valores futuros, registra um log de warning com a lista de user_id
        e seus respectivos valores futuros. Caso contrário, registra um log de info com
        mensagem de que a coluna não tem valores futuros.

        Retorno:
            pl.DataFrame: Dataframe com os usuários e suas datas futuras.
        """
        users = self.df.filter(pl.col(column) > expected_date).select(
            ["user_id", column]
        )
        users_total = len(users)
        if users_total > 0:
            message = (
                f"Usuários com {column} maior do que a data esperada: {users_total}"
            )
            log_lvl = logging.error
        else:
            message = f"Nenhuma inconsistência encontrada para a coluna {column}"
            log_lvl = logging.info
        log_lvl(message)

        return users

    def _check_min_rule(self, column, expected_min) -> pl.DataFrame:
        """
        Verifica se a coluna do DataFrame tem valores menores do que o valor esperado.

        Se houver valores menores, registra um log de warning com a lista de user_id
        e seus respectivos valores menores. Caso contrário, registra um log de info com
        mensagem de que a coluna não tem valores menores.

        Retorno:
            pl.DataFrame: Dataframe com os usuários e seus valores menores.
        """
        users = self.df.filter(pl.col(column) < expected_min).select(
            ["user_id", column]
        )
        users_total = len(users)
        if users_total < expected_min:
            message = f"Usuários com {column} < {expected_min}: {users_total}"
            log_lvl = logging.error
        else:
            message = f"Nenhum usuário com {column} negativo."
            log_lvl = logging.info
        log_lvl(message)
        return users

    def _check_employment_status_and_annual_income(self) -> pl.DataFrame:
        logging.info("Validando regra semântica da coluna employment_status")
        condition1 = pl.col("employment_status") == "Unemployed"
        condition2 = pl.col("annual_income") > 0
        users = self.df.filter(condition1 & condition2).select(
            ["user_id", "employment_status", "annual_income"]
        )
        users_total = len(users)
        if users_total > 0:
            message = f"Usuários encontrados com annual_income > 0 e employment_status == Unemployed: {len(users)}"
            log_lvl = logging.warning
        else:
            message = "Nenhum usuário encontrado com inconsistência"
            log_lvl = logging.info
        log_lvl(message)
        logging.info("Validação semântica da coluna employment_status concluída")
        return users
