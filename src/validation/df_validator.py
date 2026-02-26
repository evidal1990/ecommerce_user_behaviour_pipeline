import logging
from src.validation.interfaces.column import Column
from consts.validation_status import ValidationStatus


class DataFrameValidator:
    def __init__(self, rule_type: str, rules: list[Column]) -> None:
        self.rules = rules
        self.rule_type = rule_type

    def execute(self, df) -> dict:
        results = {}
        for rule in self.rules:
            result = rule.validate(df)
            results[rule.name()] = result
            status = result["status"]
            message = (
                f"[{self.rule_type.value}_RULES_VALIDATION]\n"
                f"rule={rule.name()}\n"
                f"status={status}\n"
                f"total_columns={result['total_columns']}\n"
                f"not_found_columns={result['not_found_columns']}\n"
                f"not_found_percentage={result['not_found_percentage']}\n"
                f"columns={result['columns']}"
            )
            log_lvl = (
                logging.info
                if status == ValidationStatus.PASS
                else (
                    logging.warning
                    if status == ValidationStatus.WARN
                    else logging.error
                )
            )
            log_lvl(message)
        return results
