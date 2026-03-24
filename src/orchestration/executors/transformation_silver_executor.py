import logging
import polars as pl
from pathlib import Path
from src.transformation.silver.enrich.enrich import EnrichData
from src.transformation.silver.clean.clean import CleanData
from src.transformation.silver.normalize.normalize import Normalize
from src.transformation.silver.clean.format import FormatData
from src.transformation.silver.clean.remove_duplicates import RemoveDuplicates
from src.transformation.silver.clean.fill_columns import FillColumns
from src.transformation.silver.enrich.columns.create_is_future_date_column import (
    CreateIsFutureDateColumn,
)
from src.transformation.silver.enrich.columns.age_group import AgeGroup
from src.transformation.silver.enrich.columns.household_size_group import (
    HouseholdSizeGroup,
)
from src.transformation.silver.enrich.columns.brand_loyalty_score_group import (
    BrandLoyaltyScoreGroup,
)
from src.transformation.silver.enrich.columns.impulse_buying_score_group import (
    ImpulseBuyingScoreGroup,
)
from src.transformation.silver.normalize.min_max_strategy import MinMaxScaling  # noqa.
from src.utils import file_io

BASE_DIR = Path(__file__).resolve().parents[3]


class TransformationSilverExecutor:
    def __init__(self, settings: dict) -> None:
        data = settings.get("data")
        if not data or "silver" not in data:
            raise ValueError("Configuração de silver não encontrada")

        self._settings = data["silver"]

    def start(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        logging.info("Transformação de dados provenientes da camada bronze iniciada")
        df = self._clean(df=df)
        df = self._normalize(df=df)
        df = self._enrich(df=df)
        self._write_silver(df=df)
        logging.info("Transformação de dados provenientes da camada bronze finalizada")
        return df

    def _clean(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        steps = [
            RemoveDuplicates(),
            FormatData(),
            FillColumns(),
        ]
        return CleanData(steps).execute(df=df)

    def _normalize(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        contract = self._load_contract()
        columns = contract["actions"]["normalize"]["columns"]
        min_max = [MinMaxScaling(column=col) for col in columns]
        return Normalize(min_max).execute(df=df)

    def _enrich(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        parent = self._settings["parent"]
        return EnrichData(
            [
                CreateIsFutureDateColumn(settings=parent, column="last_purchase_date"),
                AgeGroup(),
                HouseholdSizeGroup(),
                BrandLoyaltyScoreGroup(),
                ImpulseBuyingScoreGroup()
            ]
        ).execute(df=df)

    def _write_silver(
        self,
        df,
    ) -> None:
        path = self._settings["destination"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(path)

    def _load_contract(self) -> dict:
        path = BASE_DIR.joinpath(
            "src",
            "transformation",
            "silver",
            "schema.yaml",
        )
        try:
            return file_io.read_yaml(path)
        except FileNotFoundError:
            logging.error(f"Schema não encontrado em {path}")
            raise
