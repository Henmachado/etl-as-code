from utils.utils import get_local_spark_session

from pyspark.sql import DataFrame


spark = get_local_spark_session()


def add_customer_id(df: DataFrame) -> DataFrame:
    accounts = spark.table("accounts").select("account_id","customer_id")
    return df.join(accounts, on="account_id", how="left")


def add_country_name(df: DataFrame) -> DataFrame:
    customers = spark.table("customers").select("country_name","customer_id")
    return df.join(customers, on="customer_id", how="left")
