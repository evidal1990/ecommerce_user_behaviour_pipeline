import polars as pl
from consts.validation_status import ValidationStatus
from consts.employment_status import EmploymentStatus
from src.validation.interfaces.semantic_rule import SemanticRule


class IncomePerEmploymentStatus(SemanticRule):

    def __init__(
        self,
        status: str,
        sample_size: int = 10,
    ) -> None:
        self.status = status
        self.sample_size = sample_size

    def name(self) -> str:
        return f"EMPLOYMENT_STATUS_{self.status}"

    def sample_column(self) -> str:
        return "annual_income"

    def invalid_df(self, df: pl.DataFrame) -> pl.DataFrame:
        condition_1 = pl.col("employment_status").is_in(
            [
                EmploymentStatus.EMPLOYED.value,
                EmploymentStatus.SELF_EMPLOYED.value,
                EmploymentStatus.RETIRED.value,
            ]
        ) & (pl.col("annual_income") > 0.0)

        condition_2 = pl.col("employment_status").is_in(
            [
                EmploymentStatus.STUDENT.value,
                EmploymentStatus.UNEMPLOYED.value,
            ]
        ) & (pl.col("annual_income") == 0.0)

        return df.filter(condition_1 & condition_2).select(
            [
                "annual_income",
                "employment_status",
            ]
        )

    def decide_status(self) -> ValidationStatus:
        condition_has_passed = self._invalid_records == 0
        return ValidationStatus.PASS if condition_has_passed else ValidationStatus.WARN
