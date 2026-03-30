import polars as pl
from src.transformation.silver.enrich.enrich import EnrichData
from src.transformation.silver.enrich.columns import (
    CreateIsFutureDateColumn,
    AgeGroup,
    HouseholdSizeGroup,
    BrandLoyaltyScoreGroup,
    ImpulseBuyingScoreGroup,
    SocialMediaInfluenceScoreGroup,
    ExerciseFrequencyGroup,
    StressFromFinancialDecisionsGroup,
    OverallStressLevelGroup,
    PhysicalActivityLevelGroup,
    ReferralCountGroup,
    ImpulsePurchasesPerMonthGroup,
    BrowseToBuyRatioGroup,
    ReturnRateGroup,
    PurchaseConversionRateGroup,
    AppUsageFrequencyGroup,
    NotificationResponseRateGroup,
    SocialSharingFrequencyGroup,
    CartAbandonmentRateGroup,
    ReviewWrightingFrequencyGroup,
    AnnualIncomeGroup,
    HasChildrenGroup,
    PremiumSubscriptionGroup,
)


class EnrichExecutor:
    def __init__(self, settings: dict) -> None:
        self._settings = settings

    def start(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        columns = []
        columns.append(
            CreateIsFutureDateColumn(
                settings=self._settings,
                column="last_purchase_date",
            )
        )
        columns.append(AgeGroup())
        columns.append(HouseholdSizeGroup())
        columns.append(BrandLoyaltyScoreGroup())
        columns.append(ImpulseBuyingScoreGroup())
        columns.append(SocialMediaInfluenceScoreGroup())
        columns.append(ExerciseFrequencyGroup())
        columns.append(StressFromFinancialDecisionsGroup())
        columns.append(OverallStressLevelGroup())
        columns.append(PhysicalActivityLevelGroup())
        columns.append(ReferralCountGroup())
        columns.append(ImpulsePurchasesPerMonthGroup())
        columns.append(BrowseToBuyRatioGroup())
        columns.append(ReturnRateGroup())
        columns.append(PurchaseConversionRateGroup())
        columns.append(AppUsageFrequencyGroup())
        columns.append(NotificationResponseRateGroup())
        columns.append(SocialSharingFrequencyGroup())
        columns.append(CartAbandonmentRateGroup())
        columns.append(ReviewWrightingFrequencyGroup())
        columns.append(AnnualIncomeGroup())
        columns.append(HasChildrenGroup())
        columns.append(PremiumSubscriptionGroup())

        return EnrichData(columns).execute(df=df)
