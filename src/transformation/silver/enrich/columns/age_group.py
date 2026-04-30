import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class AgeGroup(EnrichStructure):

    def __init__(self) -> None:
        self.column = "age"

    def name(self) -> str:
        return f"{self.column.upper()}_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        labels = [
            "Alta Adoção Digital",
            "Início de Carreira",
            "Consolidamento de Carreira",
            "Estabilidade Financeira",
            "Próximas da Aposentadoria",
            "com Baixa Adoção Digital",
        ]
        return super().aggregate(df=df, column=self.column, labels=labels)
