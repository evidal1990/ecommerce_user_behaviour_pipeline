from os import dup
import polars as pl
import logging


class SemanticRules:
    def __init__(self, df: pl.DataFrame) -> None:
        self.df = df

    def execute(self) -> None:
        self._check_duplicated_user_id()

    def _check_duplicated_user_id(self) -> None:
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
