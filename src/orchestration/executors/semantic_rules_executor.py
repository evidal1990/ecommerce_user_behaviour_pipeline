import logging
import polars as pl
from datetime import datetime
from consts.employment_status import EmploymentStatus
from consts.rule_type import RuleType
from src.validation import RulesValidator
from src.validation.semantic.duplicated_user_id import DuplicatedUserId
from src.validation.semantic.employment_status_income import IncomePerEmploymentStatus
from src.validation.semantic.future_dates import FutureDates
from src.validation.semantic.min_value import MinValue


class SemanticRulesExecutor:
    def __init__(self) -> None:
        pass

    def start(self, df: pl.DataFrame) -> None:
        logging.info("Validação semântica do dataframe iniciada")
        RulesValidator(
            RuleType.SEMANTIC,
            [
                DuplicatedUserId(),
                # MinValue(
                #     column="household_size",
                #     min_limit=0,
                # ),
                # MinValue(
                #     column="travel_frequency_per_year",
                #     min_limit=0,
                # ),
                # MinValue(
                #     column="hobby_count",
                #     min_limit=0,
                # ),
                # MinValue(
                #     column="reading_habits_per_month",
                #     min_limit=0,
                # ),
                # MinValue(
                #     column="weekly_purchases",
                #     min_limit=0,
                # ),
                # MinValue(
                #     column="monthly_spend",
                #     min_limit=0.0,
                # ),
                # MinValue(
                #     column="average_order_value",
                #     min_limit=0.0,
                # ),
                # MinValue(
                #     column="referral_count",
                #     min_limit=0,
                # ),
                # MinValue(
                #     column="daily_session_time_minutes",
                #     min_limit=0,
                # ),
                # MinValue(
                #     column="product_views_per_day",
                #     min_limit=0,
                # ),
                # MinValue(
                #     column="ad_views_per_day",
                #     min_limit=0,
                # ),
                # MinValue(
                #     column="account_age_months",
                #     min_limit=0,
                # ),
                # FutureDates(
                #     column="last_purchase_date",
                #     date_limit=datetime.now().date(),
                # ),
                IncomePerEmploymentStatus(EmploymentStatus.EMPLOYED),
                IncomePerEmploymentStatus(EmploymentStatus.SELF_EMPLOYED),
                IncomePerEmploymentStatus(EmploymentStatus.UNEMPLOYED),
            ],
        ).execute(df)
        logging.info("Validação semântica do dataframe finalizada\n")
