def get_percentage(dividend: int, divider: int) -> float:
    return round((dividend / divider) * 100, 2) if dividend != divider else 0.0
