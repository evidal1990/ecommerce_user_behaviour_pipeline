import logging
import polars as pl
from typing import Any
from pathlib import Path
from src.utils import file_io


BASE_DIR = Path(__file__).resolve().parents[2]


class BusinessRulesChecks:
    def __init__(self, df: pl.DataFrame) -> None:
        self.df = df
        self._contract = self._load_contract()

    def execute(self) -> Any:
        self._check_null_count()
        self._check_columns_values()

        score_or_level_cols = self._contract["columns_to_check_score_or_level"]
        score_or_level_rule = self._contract["rules"]["score_or_level"]
        self._check_columns_rules(
            columns=score_or_level_cols, column_rules=score_or_level_rule
        )

        rate_or_ratio_cols = self._contract["columns_to_check_rate_or_ratio"]
        rate_or_ratio_rule = self._contract["rules"]["rate_or_ratio"]
        self._check_columns_rules(
            columns=rate_or_ratio_cols, column_rules=rate_or_ratio_rule
        )

        per_week_cols = self._contract["columns_to_check_frequency_per_week"]
        per_week_rule = self._contract["rules"]["per_week"]
        self._check_columns_rules(columns=per_week_cols, column_rules=per_week_rule)

        per_month_cols = self._contract["columns_to_check_frequency_per_month"]
        per_month_rule = self._contract["rules"]["per_month"]
        self._check_columns_rules(columns=per_month_cols, column_rules=per_month_rule)

        per_year_cols = self._contract["columns_to_check_frequency_per_year"]
        per_year_rule = self._contract["rules"]["per_year"]
        self._check_columns_rules(columns=per_year_cols, column_rules=per_year_rule)

    def _load_contract(self) -> dict:
        contract_path = BASE_DIR / "src" / "transformation" / "silver" / "schema.yaml"
        return file_io.read_yaml(contract_path)

    def _check_null_count(self) -> None:
        logging.info(f"Verificando total de dados ausentes por coluna...")
        for column in self.df.columns:
            null_count = self.df[column].null_count()
            if null_count > 0:
                logging.warning(
                    f"Total de dados ausentes para a coluna {column}: {null_count}"
                )
            else:
                logging.info(
                    f"Total de dados ausentes para a coluna {column}: {null_count}"
                )
        logging.info(f"Verificação de dados ausentes concluída com sucesso.")

    def _check_columns_values(self) -> None:
        logging.info("Validando lista de valores permitidos nas colunas bool e str")
        for column in self._contract["columns_to_check_values"]:
            result = [
                col
                for col in self.df.group_by(column).len()[column]
                if col not in self._contract[column]
            ]
            if len(result) > 0:
                logging.error(
                    f"Coluna {column} possui dados inexistentes no schema: {result}"
                )
            else:
                logging.info(
                    f"Valores da coluna {column} de acordo com as regras de negócio"
                )
        logging.info(
            "Validação de lista de valores permitidos nas colunas concluída com sucesso"
        )

    def _check_columns_rules(self, columns, column_rules) -> None:
        for column in columns:
            expected_min = column_rules["min"]
            received_min = self.df.select(pl.col(column)).min()[column][0]
            if received_min < expected_min:
                logging.error(
                    f"Coluna {column} com valor mínimo fora do range: {received_min}"
                )
            else:
                logging.info(f"Coluna {column} com valor mínimo dentro do range.")
            expected_max = column_rules["max"]
            received_max = self.df.select(pl.col(column)).max()[column][0]
            if received_max > expected_max:
                logging.error(
                    f"Coluna {column} com valor máximo fora do range: {received_max}"
                )
            else:
                logging.info(f"Coluna {column} com valor máximo dentro do range.")
