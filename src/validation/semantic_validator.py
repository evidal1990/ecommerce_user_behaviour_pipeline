from validation.semantic_rule import SemanticRule


class SemanticRulesValidator:
    def __init__(self, rules: list[SemanticRule]) -> None:
        self.rules = rules

    def validate(self, df) -> dict:
        return {rule.name: rule.validate(df) for rule in self.rules}
