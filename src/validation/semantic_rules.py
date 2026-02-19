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
            logging.error(f"user_id duplicados encontrados: {duplicated_ids}")
        else:
            logging.info("Coluna user_id sem valores duplicados")
        logging.info("Verificação de duplicidade de user_id concluida.")
