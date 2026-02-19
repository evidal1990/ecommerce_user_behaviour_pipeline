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

    def execute(self) -> pl.DataFrame:
        """
        Executa o pipeline de validação de regras de negócios.

        Valida se as colunas do DataFrame apresentam valores nulos,
        valores fora do esperado e regras de negócios.

        - Valida se as colunas do DataFrame apresentam valores nulos.
        - Valida se as colunas do DataFrame apresentam valores fora do esperado.
        - Valida se as colunas do DataFrame atendem as regras de negócios.

        Retorno:
            pl.DataFrame: Dataframe com os dados validados pelas regras de negócios.
        """
        self._check_null_count()
        self._check_columns_values()
        self._check_columns_rules()

        return self.df

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
        """
        Valida se as colunas do DataFrame apresentam valores permitidos.

        Se houver divergências, registra um log de error com as colunas
        e seus respectivos valores divergentes.

        Retorno:
            None
        """
        logging.info("Validando lista de valores permitidos nas colunas bool e str")

        columns = self._contract["columns_to_check_values"]

        df_subset = self.df.select(columns)

        for column in columns:
            invalids = set(df_subset[column].unique().to_list()) - set(
                self._contract[column]
            )

            if invalids:
                logging.error(
                    f"Coluna {column} possui dados inexistentes no schema: {sorted(invalids)}"
                )
            else:
                logging.info(
                    f"Valores da coluna {column} de acordo com as regras de negócio"
                )
        logging.info(
            "Validação de lista de valores permitidos nas colunas concluída com sucesso"
        )

    def _check_columns_rules(self) -> None:
        """
        Valida se as colunas do DataFrame atendem as regras de negócios
        de valores mínimos e máximos.

        Se houver divergências, registra um log de error com as colunas
        e seus respectivos valores divergentes.

        Retorno:
            None
        """
        logging.info("Validando regras de negócio de valores mínimos e máximos")

        columns = self._contract["columns_to_check_min_max_values"]

        exprs = []
        for col in columns:
            exprs.extend(
                [
                    pl.col(col).min().alias(f"{col}__min"),
                    pl.col(col).max().alias(f"{col}__max"),
                ]
            )

        stats = self.df.select(exprs).row(0)
        stats_map = dict(zip(self.df.select(exprs).columns, stats))

        for column in columns:
            rules = self._contract[column]

            received_min = stats_map[f"{column}__min"]
            if received_min >= rules["min"]:
                logging.info(f"Coluna {column}: mínimo recebido OK")
            else:
                logging.error(
                    f"Coluna {column}: mínimo esperado {rules["min"]} mínimo recebido {received_min}"
                )
            received_max = stats_map[f"{column}__max"]
            if received_max <= rules["max"]:
                logging.info(f"Coluna {column}: máximo recebido OK")
            else:
                logging.error(
                    f"Coluna {column}: máximo esperado {rules["max"]} máximo recebido {received_max}"
                )
        logging.info(
            "Validação de regras de negócio de valores mínimos e máximos concluída"
        )
