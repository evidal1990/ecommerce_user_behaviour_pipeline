import polars as pl
from pathlib import Path
from src.utils import file_io, dataframe, statistics
from src.validation.interfaces.rule import Rule
from consts.validation_status import ValidationStatus


BASE_DIR = Path(__file__).resolve().parents[3]


class AllowedMinMaxValues(Rule):
    def __init__(self, column: str, sample_size: int = 10) -> None:
        self.column = column
        self.sample_size = sample_size

    def name(self) -> str:
        return f"allowed_min_values_{self.column}"

    def validate(self, df: pl.DataFrame) -> dict:
        total_records = df.shape[0]
        contract = self._load_contract()
        condition_1 = pl.col(self.column).min() < contract[self.column]["min"]
        condition_2 = pl.col(self.column).max() > contract[self.column]["max"]
        users = df.filter(condition_1 | condition_2).select([self.column])
        users_total = len(users)
        if users_total == 0:
            status = ValidationStatus.PASS
            sample = []
            percentage = 0.0
        else:
            status = ValidationStatus.FAIL
            sample = dataframe.get_df_sample(
                df=users, column=self.column, sample_size=self.sample_size
            )
            percentage = statistics.get_percentage(
                dividend=users_total, divider=total_records
            )
        return {
            "status": status,
            "total_records": total_records,
            "invalid_records": users_total,
            "invalid_percentage": percentage,
            "sample": sample,
        }

    def _load_contract(self) -> dict:
        """
        Carrega o contrato de regras de negócios, que é um arquivo YAML
        com as configurações de regras de negócios.

        Retorno:
            dict: Contrato de regras de negócios.
        """
        contract_path = BASE_DIR / "src" / "transformation" / "silver" / "schema.yaml"
        return file_io.read_yaml(contract_path)
