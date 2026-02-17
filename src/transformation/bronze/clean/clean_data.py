import logging
import polars as pl
from typing import Any
from pathlib import Path
from src.utils import file_io, dataframe


BASE_DIR = Path(__file__).resolve().parents[4]


class CleanRawData:
    def __init__(self, df: pl.DataFrame) -> None:
        self.df = df
        self._contract = self._load_contract()

    def execute(self) -> Any:
        self._check_null_count()
        self._check_bool_column_values()
        self._check_str_column_values()

    def _load_contract(self) -> dict:
        contract_path = (
            BASE_DIR / "src" / "transformation" / "bronze" / "clean" / "schema.yaml"
        )
        return file_io.read_yaml(contract_path)

    def _check_null_count(self) -> None:
        logging.info(f"Verificando total de dados ausentes por coluna...")
        for column in self.df.columns:
            null_count = self.df[column].null_count()
            level = logging.warning if null_count > 0 else logging.info
            level(f"{column}: {null_count}")
        logging.info(f"Verificação de dados ausentes concluída com sucesso.")

    def _check_bool_column_values(self) -> None:
        dataframe.check_column_values(
            df=self.df, 
            column_values=self._contract["has_children"], 
            column="has_children"
        )
        dataframe.check_column_values(
            df=self.df, 
            column_values=self._contract["has_environmental_consciousness"], 
            column="has_environmental_consciousness"
        )
        dataframe.check_column_values(
            df=self.df, 
            column_values=self._contract["has_health_conscious_shopping"], 
            column="has_health_conscious_shopping"
        )
        dataframe.check_column_values(
            df=self.df, 
            column_values=self._contract["is_weekend_shopper"], 
            column="is_weekend_shopper"
        )
        dataframe.check_column_values(
            df=self.df, 
            column_values=self._contract["is_loyalty_program_member"], 
            column="is_loyalty_program_member"
        )
        logging.info(f"Validação dos valores de colunas bool concluída com sucesso.")
    def _check_str_column_values(self) -> None:
        dataframe.check_column_values(
            df=self.df, 
            column_values=self._contract["gender"], 
            column="gender"
        )
        dataframe.check_column_values(
            df=self.df,
            column_values=self._contract["employment_status"],
            column="employment_status",
        )
        dataframe.check_column_values(
            df=self.df,
            column_values=self._contract["urban_rural"],
            column="urban_rural",
        )
        dataframe.check_column_values(
            df=self.df,
            column_values=self._contract["education_level"],
            column="education_level",
        )
        dataframe.check_column_values(
            df=self.df,
            column_values=self._contract["relationship_status"],
            column="relationship_status",
        )
        dataframe.check_column_values(
            df=self.df,
            column_values=self._contract["device_type"],
            column="device_type",
        )
        dataframe.check_column_values(
            df=self.df,
            column_values=self._contract["ethnicity"],
            column="ethnicity",
        )
        dataframe.check_column_values(
            df=self.df,
            column_values=self._contract["budgeting_style"],
            column="budgeting_style",
        )
        dataframe.check_column_values(
            df=self.df,
            column_values=self._contract["preferred_payment_method"],
            column="preferred_payment_method",
        )
        dataframe.check_column_values(
            df=self.df,
            column_values=self._contract["product_category_preference"],
            column="product_category_preference",
        )
        dataframe.check_column_values(
            df=self.df,
            column_values=self._contract["shopping_time_of_day"],
            column="shopping_time_of_day",
        )
        logging.info(f"Validação dos valores de colunas str concluída com sucesso.")
