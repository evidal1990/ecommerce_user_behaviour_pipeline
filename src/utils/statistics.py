def get_percentage(dividend: int, divider: int) -> float:
    return round(dividend / divider * 100, 2) if divider > 0 else 0.0
