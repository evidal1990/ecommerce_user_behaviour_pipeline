from enum import Enum
from logging import CRITICAL


class ValidationStatus(Enum):
    FAIL = "FAIL"
    PASS = "PASS"
    WARN = "WARN"
    CRITICAL = "CRITICAL"
