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
        for column in [
            "has_children",
            "has_environmental_consciousness",
            "has_health_conscious_shopping",
            "is_weekend_shopper",
            "is_loyalty_program_member",
            "gender",
            "employment_status",
            "urban_rural",
            "education_level",
            "relationship_status",
            "device_type",
            "ethnicity",
            "budgeting_style",
            "preferred_payment_method",
            "product_category_preference",
            "shopping_time_of_day",
        ]:
            self._check_column_values(column)

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

    def _check_column_values(self, column) -> None:
        logging.info(
            "Validando lista de valores permitidos nas colunas (regras de negócio)"
        )
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
