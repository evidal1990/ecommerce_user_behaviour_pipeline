import logging
from typing import Any


class CleanData:
    def __init__(self, df) -> None:
        self.df = df

    def execute(self) -> Any:
        return self.df

    def _delete_columns(self, columns) -> None:
        if not columns:
            raise ValueError("Nenhuma coluna informada para exclusão.")
        self.df.drop(columns)
        logging.info(f"Colunas excluídas com sucesso: {columns}.")

    def _clean_data(self) -> None:
        missing_data = self.df.isna().sum()
        logging.info(f"Quantidade de dados ausentes: {missing_data}")
