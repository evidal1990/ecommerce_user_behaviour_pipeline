import polars as pl
from src.transformation.silver.enrich.enrich_structure import EnrichStructure


class ExerciseFrequencyGroup(EnrichStructure):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "EXERCISE_FREQUENCY_PER_WEEK_GROUP"

    def execute(
        self,
        df,
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col("exercise_frequency_per_week")
            .map_elements(self._classify)
            .alias(self.name().lower())
        )

    def _classify(
        self,
        exercise_frequency: int,
    ) -> str:
        if exercise_frequency < 0:
            return "Other"
        elif exercise_frequency == 0:
            return "Inactive"
        elif exercise_frequency <= 2:
            return "Lightly Active"
        elif exercise_frequency <= 4:
            return "Active"
        return "Highly Active"
