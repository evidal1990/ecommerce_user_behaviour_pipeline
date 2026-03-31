import polars as pl


class CreateKpis:

    def __init__(
        self,
        kpis: list,
        standard_columns: list,
    ) -> None:
        self.kpis = kpis
        self.standard_columns = standard_columns

    def execute(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:

        return pl.concat(
            [self._standardize_schema(kpi.calculate(df)) for kpi in self.kpis]
        )

    def build_kpis(self, configs: list) -> list:
        kpis = []

        for cfg in configs:
            kpi_class = cfg["class"]

            dimensions = cfg.get("dimensions")
            if not dimensions:
                dimensions = [cfg["dimension"]]

            group_by_list = cfg.get("group_by", [])

            for dimension in dimensions:

                # normaliza dimension (garantia)
                if isinstance(dimension, list):
                    raise ValueError(
                        f"dimension deve ser string, recebido: {dimension}"
                    )

                # sem group_by
                if not group_by_list:
                    kpis.append(
                        kpi_class(
                            dimension=dimension,
                            group_by=[],
                        )
                    )
                    continue

                for group in group_by_list:

                    # 🔥 aqui está o ponto crítico
                    if isinstance(group, list):
                        group_cols = group  # já é lista válida
                    else:
                        group_cols = [group]  # transforma string em lista

                    kpis.append(
                        kpi_class(
                            dimension=dimension,
                            group_by=group_cols,
                        )
                    )

        return kpis

    def _standardize_schema(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        for col in self.standard_columns:
            if col not in df.columns:
                default_value = None if col == "metric_value" else "All"
                df = df.with_columns(pl.lit(default_value).alias(col))

        df = df.with_columns(
            [
                pl.col(col).cast(pl.Utf8)
                for col in self.standard_columns
                if col != "metric_value"
            ]
            + [pl.col("metric_value").cast(pl.Float64)]
        )

        return df.select(self.standard_columns)
