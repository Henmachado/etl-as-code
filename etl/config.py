from dataclasses import dataclass

from utils.utils import get_local_spark_session

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


spark = get_local_spark_session()


@dataclass
class StaggingTransformations:
    """
    Provide a proper map to identify the columns that will be standardize in the stagging layer
    """
    account_id_col: str
    amount_col: str
    requested_at_col: str
    completed_at_col: str
    status_col: str
    in_or_out_col: str
    product: str
    

    def generate_columns(self) -> list:
        """Factory method to create all new columns"""
        return [
            F.col(self.account_id_col).alias("account_id"),
            F.col(self.amount_col).alias("amount"),
            F.col(self.requested_at_col).alias("txn_requested_at"),
            F.col(self.completed_at_col).alias("txn_completed_at"),
            F.col(self.status_col).alias("status"),
            F.when(F.col(self.in_or_out_col).like("%_in%"), F.lit("in")).otherwise(F.lit("out")).alias("in_or_out"),
            F.lit(self.product).alias("product"),
        ]


def get_stagging_transformations(config: StaggingTransformations, df: DataFrame) -> DataFrame:
    return (
        df.select(*config.generate_columns())
          .withColumn("day",F.dayofmonth(F.from_unixtime("txn_requested_at")))
          .withColumn("month",F.date_format(F.from_unixtime("txn_requested_at"),"yyyy-MM"))
          .withColumn("event_date",F.date_format(F.from_unixtime("txn_requested_at"),"yyyy-MM-dd"))
    )


def add_customer_id(df: DataFrame) -> DataFrame:
    accounts = spark.table("accounts").select("account_id","customer_id")
    return df.join(accounts, on="account_id", how="left")


def add_country_name(df: DataFrame) -> DataFrame:
    customers = spark.table("customers").select("country_name","customer_id")
    return df.join(customers, on="customer_id", how="left")
