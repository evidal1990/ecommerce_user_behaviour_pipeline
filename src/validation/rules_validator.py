import logging
from src.validation.interfaces.rule import Rule
from consts.validation_status import ValidationStatus


class RulesValidator():
    def __init__(self, rule_type: str, rules: list[Rule]) -> None:
        self.rule_type = rule_type
        self.rules = rules

    def execute(self, df) -> dict:
        results = {}
        for rule in self.rules:
            result = rule.validate(df)
            results[rule.name()] = result
            status = result["status"]
            message = (
                f"[{self.rule_type.value}_RULES_VALIDATION] "
                f"rule={rule.name()} "
                f"status={status} "
                f"total_records={result['total_records']} "
                f"invalid_records={result['invalid_records']} "
                f"invalid_percentage={result['invalid_percentage']} "
                f"sample={result['sample']}"
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
