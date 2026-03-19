import logging
import polars as pl


class RemoveDuplicates:

    def __init__(self) -> None:
        pass

    def execute(self, df: pl.DataFrame) -> pl.DataFrame:
        df_height_before_cleaning = df.height
        df = df.with_row_index("row_id").unique(subset=["user_id"], keep="first",)
        duplicated_total = df_height_before_cleaning - df.height
        
        logging.info(
            (
                f"DATA_CLEANING_REMOVE_DUPLICATED\n"
                f"Registros duplicados removidos: {duplicated_total}\n"
            )
        )
        return df
