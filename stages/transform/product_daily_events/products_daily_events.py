from utils.utils import get_local_spark_session

from stages.extract.stagging_configs import StaggingProductDailyEvents
from stages.extract.stagging_configs import get_stagging_transformations

from stages.transform.product_daily_events.joins import add_customer_id
from stages.transform.product_daily_events.joins import add_country_name

from pyspark.sql import functions as F


spark = get_local_spark_session()


def create_stg_daily_events_pix_df() -> None:
    
    daily_events_pix = StaggingProductDailyEvents(
        source_table = spark.table("pix_movements"),
        account_id_col="account_id",
        amount_col="pix_amount",
        requested_at_col="pix_requested_at",
        completed_at_col="pix_completed_at",
        status_col="status",
        in_or_out_col="in_or_out",
        product="pix"
    )

    daily_events_pix_df = (
        get_stagging_transformations(config=daily_events_pix)
        .transform(add_customer_id)
        .transform(add_country_name)
    )

    return daily_events_pix_df


def create_stg_daily_events_transfer_in_df() -> None:

    daily_events_transfer_in = StaggingProductDailyEvents(
        source_table = spark.table("transfer_ins").withColumn("in_or_out", F.lit("non_pix_in")),
        account_id_col="account_id",
        amount_col="amount",
        requested_at_col="transaction_requested_at",
        completed_at_col="transaction_completed_at",
        status_col="status",
        in_or_out_col="in_or_out",
        product="non_pix",
    )

    daily_events_transfer_in_df = (
        get_stagging_transformations(config=daily_events_transfer_in)
        .transform(add_customer_id)
        .transform(add_country_name)
    )

    return daily_events_transfer_in_df


def create_stg_daily_events_transfer_outs_df() -> None:
    
    daily_events_transfer_outs = StaggingProductDailyEvents(
        source_table = spark.table("transfer_outs").withColumn("in_or_out", F.lit("non_pix_in")),
        account_id_col="account_id",
        amount_col="amount",
        requested_at_col="transaction_requested_at",
        completed_at_col="transaction_completed_at",
        status_col="status",
        in_or_out_col="in_or_out",
        product="non_pix",
    )

    daily_events_transfer_outs_df = (
        get_stagging_transformations(config=daily_events_transfer_outs)
        .transform(add_customer_id)
        .transform(add_country_name)
    )

    return daily_events_transfer_outs_df


def create_stg_daily_events_investments_df() -> None:
    
    daily_events_investments = StaggingProductDailyEvents(
        source_table = spark.table("investments").withColumn("in_or_out", F.lit("non_pix_in")),
        account_id_col="account_id",
        amount_col="amount",
        requested_at_col="transaction_requested_at",
        completed_at_col="transaction_completed_at",
        status_col="status",
        in_or_out_col="in_or_out",
        product="non_pix",
    )

    daily_events_investments_df = (
        get_stagging_transformations(config=daily_events_investments)
        .transform(add_customer_id)
        .transform(add_country_name)
    )

    return daily_events_investments_df

