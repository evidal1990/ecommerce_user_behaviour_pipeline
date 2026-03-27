from src.transformation.gold.metrics.percentage_structure import PercentageStructure


class PercentageUsersByDeviceType(PercentageStructure):
    def __init__(self) -> None:
        super().__init__(column="device_type")
