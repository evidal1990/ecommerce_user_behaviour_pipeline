from enum import Enum


class IngestionStatus(Enum):
    FAIL = 0
    PASS = 1
    WARN = 2
