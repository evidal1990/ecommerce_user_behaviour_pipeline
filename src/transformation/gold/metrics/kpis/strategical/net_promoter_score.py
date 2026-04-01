import polars as pl
import logging
from src.transformation.gold.metrics.strcutures.base_structure import (
    BaseStructure,
)


class NetPromoterScore(BaseStructure):
    def __init__(
        self,
        dimension: str,
        group_by: list[str] = [],
    ) -> None:
        super().__init__(
            metric="net_promoter_score",
            metric_type="composite",
            dimension_col=dimension,
            group_cols=group_by,
        )

    def calculate(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        df_agg = self.aggregate_with_total(
            df=df,
        )
        df_nps = self.apply_nps_formula(df_agg)
        return self._finalize_output(df_nps)

    def build_condition_counts(
        self, column: str, conditions: dict[str, str]
    ) -> list[pl.Expr]:
        conditions = {
            "promoters": "Promoters",
            "detractors": "Detractors",
        }
        return [
            (pl.col(column) == value).sum().alias(name)
            for name, value in conditions.items()
        ]

    def aggregate_with_total(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        return (
            df.group_by(self.group_cols)
            .agg(
                [
                    pl.when(pl.col("brand_loyalty_score_group") == "Promoters")
                    .then(pl.col("count_users"))
                    .otherwise(0)
                    .sum()
                    .alias("promoters"),
                    pl.when(pl.col("brand_loyalty_score_group") == "Detractors")
                    .then(pl.col("count_users"))
                    .otherwise(0)
                    .sum()
                    .alias("detractors"),
                    pl.col("count_users").sum().alias("total"),
                ]
            )
            .with_columns(
                ((pl.col("promoters") - pl.col("detractors")) / pl.col("total")).alias(
                    "nps"
                )
            )
        )

    def apply_nps_formula(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        nps_ratio = (
            pl.col("promoters").cast(pl.Int64) - pl.col("detractors").cast(pl.Int64)
        ) / pl.col("total")

        return df.with_columns(
            (((nps_ratio + 1) / 2) * 100).round(2).alias("metric_value")
        )

    def _finalize_output(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        return df.with_columns(
            [
                pl.lit(self.metric).alias("metric"),
                pl.lit(self.metric_type).alias("metric_type"),
                pl.lit(self.dimension_col).alias("dimension"),
                pl.lit("All").alias("value"),
            ]
        ).select(
            [
                "metric",
                "metric_type",
                "dimension",
                "value",
                *self.group_cols,
                "metric_value",
            ]
        )
