from .s3_client import s3_client
import logging
import os
from .get_latest_files import get_latest_files
from pprint import pprint as pp

from .list_s3 import ingested_data_retrival
from .transform import (
    transform_fact_sales_order,
    transform_dim_staff,
    transform_dim_location,
    transform_dim_design,
    transform_dim_date,
    transform_dim_currency,
    transform_dim_counterparty,
    transform_fact_purchase_order,
    transform_fact_payment,
    transform_dim_payment_type,
    transform_dim_transaction,
)


def star_schema(client, ingested_bucket_name, logger, files_dict):  # noqa
    """
    Transforms ingested data into a star schema format.
    Args:
        client (object): The client object used for data retrieval.
        ingested_bucket_name (str): The name of the bucket where
        ingested data is stored.
        logger (object): Logger object for logging information and errors.
        files_dict (dict): Dictionary containing file paths
        and related information.
    Returns:
        dict: A dictionary containing transformed dataframes
        in star schema format.
    Raises:
        Exception: If required dataframes are not available or any
                    error occurs during transformation.
    The returned dictionary contains the following keys:
        - "fact_sales_order": Transformed sales order fact dataframe.
        - "fact_purchase_order": Transformed purchase order fact dataframe.
        - "fact_payment": Transformed payment fact dataframe.
        - "dim_staff": Transformed staff dimension dataframe.
        - "dim_location": Transformed location dimension dataframe.
        - "dim_design": Transformed design dimension dataframe.
        - "dim_date": Transformed date dimension dataframe.
        - "dim_currency": Transformed currency dimension dataframe.
        - "dim_counterparty": Transformed counterparty dimension dataframe.
        - "dim_payment_type": Transformed payment type dimension dataframe.
        - "dim_transaction": Transformed transaction dimension dataframe.
    """
    required_dataframes = [
        "df_address",
        "df_counterparty",
        "df_currency",
        "df_department",
        "df_design",
        "df_payment",
        "df_payment_type",
        "df_purchase_order",
        "df_sales_order",
        "df_staff",
        "df_transaction",
    ]
    dataframes = ingested_data_retrival(
        client, files_dict, logger, ingested_bucket_name
    )
    available_dataframes = list(dataframes.keys())
    try:
        if set(required_dataframes).issubset(available_dataframes):
            logger.info("All required dataframes are available")

            df_fact_sales_order = transform_fact_sales_order(
                dataframes["df_sales_order"]
            )
            df_fact_purchase_order = transform_fact_purchase_order(
                dataframes["df_purchase_order"]
            )
            df_fact_payment = transform_fact_payment(dataframes["df_payment"])
            dim_staff = transform_dim_staff(
                dataframes["df_staff"], dataframes["df_department"]
            )
            dim_location = transform_dim_location(dataframes["df_address"])
            dim_design = transform_dim_design(dataframes["df_design"])
            dim_date = transform_dim_date()
            dim_currency = transform_dim_currency(dataframes["df_currency"])
            dim_counterparty = transform_dim_counterparty(
                dataframes["df_counterparty"], dataframes["df_address"]
            )
            dim_payment_type = transform_dim_payment_type(
                dataframes["df_payment_type"]
            )  # noqa
            dim_transaction = transform_dim_transaction(
                dataframes["df_transaction"]
            )  # noqa
            df_star_schema = {
                "fact_sales_order": df_fact_sales_order,
                "fact_purchase_order": df_fact_purchase_order,
                "fact_payment": df_fact_payment,
                "dim_staff": dim_staff,
                "dim_location": dim_location,
                "dim_design": dim_design,
                "dim_date": dim_date,
                "dim_currency": dim_currency,
                "dim_counterparty": dim_counterparty,
                "dim_payment_type": dim_payment_type,
                "dim_transaction": dim_transaction,
            }
            logger.info("Star-schema data successfully processed")
            return df_star_schema
    except Exception:
        logger.error("Insuffcient dataframes for star-schema data processing")
        raise


client = s3_client()
ingested_bucket_name = os.environ.get("ingested_data_bucket")
transform_bucket_name = os.environ.get("processed_data_bucket")
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
files_dict = get_latest_files(client, ingested_bucket_name, logger)

df_star_schema = star_schema(client, ingested_bucket_name, logger, files_dict)
pp(df_star_schema)
