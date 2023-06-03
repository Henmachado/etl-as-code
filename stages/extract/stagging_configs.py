from abc import ABC, abstractmethod

from dataclasses import dataclass

from utils.utils import get_local_spark_session

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


spark = get_local_spark_session()


class StaggingTransformations(ABC):

    @abstractmethod
    def generate_columns(self) -> List:
        pass


@dataclass
class StaggingProductDailyEvents(StaggingTransformations):
    """
    Provide a proper map to identify the columns that will be standardize in the stagging layer
    """
    source_table: DataFrame
    account_id_col: str
    amount_col: str
    requested_at_col: str
    completed_at_col: str
    status_col: str
    in_or_out_col: str
    product: str
    

    def generate_columns(self) -> List:
        """Factory method to create all new columns"""
        return [
            F.col(self.account_id_col).alias("account_id"),
            F.col(self.amount_col).alias("amount"),
            F.col(self.requested_at_col).alias("txn_requested_at"),
            F.col(self.completed_at_col).alias("txn_completed_at"),
            F.col(self.status_col).alias("status"),
            F.when(F.col(self.in_or_out_col).like("%_in%"), F.lit("in")).otherwise(F.lit("out")).alias("in_or_out"),
            F.lit(self.product).alias("product"),
            F.dayofmonth(F.from_unixtime(self.requested_at_col)).alias("day"),
            F.date_format(F.from_unixtime(self.requested_at_col),"yyyy-MM").alias("month"),
            F.date_format(F.from_unixtime(self.requested_at_col),"yyyy-MM-dd").alias("event_date"),
        ]


def get_stagging_transformations(config: StaggingTransformations) -> DataFrame:
    df = config.source_table
    return df.select(*config.generate_columns())
