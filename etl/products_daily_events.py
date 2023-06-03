from utils.utils import get_local_spark_session

from pyspark.sql import functions as F

from etl.config import (
    StaggingTransformations, 
    get_stagging_transformations, 
    add_customer_id, 
    add_country_name,
)


spark = get_local_spark_session()


def create_daily_events_pix_df() -> None:
    pix_movements = spark.table("pix_movements")

    daily_events_pix = StaggingTransformations(
        account_id_col="account_id",
        amount_col="pix_amount",
        requested_at_col="pix_requested_at",
        completed_at_col="pix_completed_at",
        status_col="status",
        in_or_out_col="in_or_out",
        product="pix"
    )

    daily_events_pix_df = (
        get_stagging_transformations(config=daily_events_pix, df=pix_movements)
        .transform(add_customer_id)
        .transform(add_country_name)
    )

    return daily_events_pix_df


def create_daily_events_transfer_in_df() -> None:
    transfer_ins = spark.table("transfer_ins").withColumn("in_or_out", F.lit("non_pix_in"))

    daily_events_transfer_in = StaggingTransformations(
        account_id_col="account_id",
        amount_col="amount",
        requested_at_col="transaction_requested_at",
        completed_at_col="transaction_completed_at",
        status_col="status",
        in_or_out_col="in_or_out",
        product="non_pix",
    )

    daily_events_transfer_in_df = (
        get_stagging_transformations(config=daily_events_transfer_in, df=transfer_ins)
        .transform(add_customer_id)
        .transform(add_country_name)
    )

    return daily_events_transfer_in_df


def create_daily_events_transfer_outs_df() -> None:
    transfer_outs = spark.table("transfer_outs").withColumn("in_or_out", F.lit("non_pix_in"))

    daily_events_transfer_outs = StaggingTransformations(
        account_id_col="account_id",
        amount_col="amount",
        requested_at_col="transaction_requested_at",
        completed_at_col="transaction_completed_at",
        status_col="status",
        in_or_out_col="in_or_out",
        product="non_pix",
    )

    daily_events_transfer_outs_df = (
        get_stagging_transformations(config=daily_events_transfer_outs, df=transfer_outs)
        .transform(add_customer_id)
        .transform(add_country_name)
    )

    return daily_events_transfer_outs_df


def create_daily_events_investments_df() -> None:
    investments = spark.table("investments").withColumn("in_or_out", F.lit("non_pix_in"))

    daily_events_investments = StaggingTransformations(
        account_id_col="account_id",
        amount_col="amount",
        requested_at_col="transaction_requested_at",
        completed_at_col="transaction_completed_at",
        status_col="status",
        in_or_out_col="in_or_out",
        product="non_pix",
    )

    daily_events_investments_df = (
        get_stagging_transformations(config=daily_events_investments, df=investments)
        .transform(add_customer_id)
        .transform(add_country_name)
    )

    return daily_events_investments_df

