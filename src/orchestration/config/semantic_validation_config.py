from datetime import datetime


SEMANTIC_MIN_VALUE_COLUMNS = {
    "annual_income": 0.0,
    "household_size": 0,
    "monthly_spend": 0.0,
    "average_order_value": 0.0,
    "daily_session_time_minutes": 0,
    "cart_items_average": 0,
    "account_age_months": 0,
}

DATE_COLUMNS = {"last_purchase_date": datetime.now().date()}
