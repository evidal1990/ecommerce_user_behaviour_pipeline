import logging
import polars as pl
from typing import Any
from pathlib import Path
from src.utils import file_io


BASE_DIR = Path(__file__).resolve().parents[2]
ALLOWED_RANGE_COLUMNS = [
    "stress_from_financial_decisions_level",
    "sleep_quality_level",
    "physical_activity_level",
    "overall_stress_level",
    "mental_health_score",
    "social_media_influence_score",
    "impulse_buying_score",
    "brand_loyalty_score",
    "browse_to_buy_ratio",
    "cart_abandonment_rate",
    "notification_response_rate",
    "purchase_conversion_rate",
    "return_rate",
    "exercise_frequency_per_week",
    "reading_habits_per_month",
    "impulse_purchases_per_month",
    "checkout_abandonments_per_month",
    "travel_frequency_per_year",
]
ALLOWED_VALUES_COLUMNS = [
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
]


class BusinessRulesChecks:
    def __init__(self, df: pl.DataFrame) -> None:
        self.df = df
        self._contract = self._load_contract()

    def execute(self) -> pl.DataFrame:
        """
        Executa as verificações de regras de negócios.

        Verifica se as colunas do DataFrame tem valores nulos, se os valores
        estão dentro de um range especificado e se os valores estão dentro
        de uma lista de valores permitidos.

        Retorna o DataFrame com as verificações concluídas.
        """
        logging.info("Verificando valores permitidos por coluna")
        for key, value in self._contract.items():
            logging.info(f"Coluna {key}")
            assert isinstance(key, str)
            assert isinstance(value, dict)
            self._check_null_count(key)
            match key:
                case _ if key in ALLOWED_RANGE_COLUMNS:
                    self._check_min_value(key, value["min"])
                    self._check_max_value(key, value["max"])

                case _ if key in ALLOWED_VALUES_COLUMNS:
                    self._check_allowed_values(key, value["values"])

                case _:
                    raise ValueError(
                        "Regra de negócio não reconhecida para a coluna em questão."
                    )
        logging.info("Verificação concluída")

        return self.df

    def _load_contract(self) -> dict:
        """
        Carrega o contrato de regras de negócios, que é um arquivo YAML
        com as configurações de regras de negócios.

        Retorno:
            dict: Contrato de regras de negócios.
        """
        contract_path = BASE_DIR / "src" / "transformation" / "silver" / "schema.yaml"
        return file_io.read_yaml(contract_path)

    def _check_null_count(self, key) -> None:
        """
        Verifica se a coluna do DataFrame tem valores nulos.

        Registra um log de warning se houver valores nulos e um log de info se os valores forem de acordo com o esperado.

        Parametros:
            key (str): Nome da coluna a ser verificada.

        Retorno:
            None
        """
        null_count = self.df[key].null_count()
        log_lvl = logging.warning if null_count > 0 else logging.info
        log_lvl(f"Total de dados ausentes: {null_count}")

    def _check_allowed_values(self, key, values) -> None:
        """
        Verifica se os valores presentes em uma coluna do DataFrame são iguais aos valores permitidos pela regra de negócio.

        Registra um log de warning se houver valores inválidos e um log de info se os valores forem de acordo com as regras de negócio.

        Parametros:
            key (str): Nome da coluna a ser verificada.
            values (list[str]): Lista de valores permitidos pela regra de negócio.

        Retorno:
            None
        """
        invalids = set(self.df[key]) - set(values)
        if invalids:
            message = f"Valores inválidos encontrados no dataset: {sorted(invalids)}"
            log_lvl = logging.error
        else:
            message = f"Valores encontrados estão de acordo com as regras."
            log_lvl = logging.info
        log_lvl(message)

    def _check_min_value(self, key, value) -> None:
        """
        Verifica se o valor mínimo de uma coluna do DataFrame é maior ou igual
        ao valor mínimo esperado pela regra de negócio.

        Registra um log de warning se o valor mínimo for menor ao valor
        esperado e um log de info se for maior ou igual.

        Parametros:
            key (str): Nome da coluna a ser verificada.
            value (dict): Dicionário com as configurações de tipo de dados da coluna.

        Retorno:
            None
        """
        min_val = self.df.select(pl.col(key).min())[key][0]
        log_lvl = logging.error if min_val < value else logging.info
        log_lvl(f"Mínimo esperado: {value} ; Mínimo recebido: {min_val}")

    def _check_max_value(self, key, value) -> None:
        """
        Verifica se o valor máximo de uma coluna do DataFrame é menor ou igual
        ao valor máximo esperado pela regra de negócio.

        Registra um log de warning se o valor máximo for maior do que o valor
        esperado e um log de info se for menor ou igual.

        Parametros:
            key (str): Nome da coluna a ser verificada.
            value (dict): Dicionário com as configurações de tipo de dados da coluna.

        Retorno:
            None
        """
        max_val = self.df.select(pl.col(key).max())[key][0]
        log_lvl = logging.error if max_val > value else logging.info
        log_lvl(f"Máximo esperado: {value} ; Máximo recebido: {max_val}")
