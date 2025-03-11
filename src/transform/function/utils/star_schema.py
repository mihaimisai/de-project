from .list_s3 import ingested_data_retrival
from .transform import (
    transform_fact_sales_order,
    transform_dim_staff,
    transform_dim_location,
    transform_dim_design,
    transform_dim_date,
    transform_dim_currency,
    transform_dim_counterparty,
)


def star_schema(client, ingested_data_bucket, logger, files_dict):
    """

    Arguments:
        client : s3 client,
        ingested_data_bucket : bucket name passed to ingested_data_retrieval
        logger : the logger for logging messages
        files_dict : dictionary containing table name and path to file

    Returns:
        dictionary with keys as dataframe names and values as
        parquet dataframes
    """
    required_dataframes = [
        "df_sales_order",
        "df_staff",
        "df_department",
        "df_address",
        "df_design",
        "df_currency",
        "df_counterparty",
    ]
    dataframes = ingested_data_retrival(
        client, files_dict, logger, ingested_data_bucket
    )
    available_dataframes = list(dataframes.keys())
    try:
        if set(required_dataframes).issubset(available_dataframes):
            logger.info("All required dataframes are available")

            df_fact_sales_order = transform_fact_sales_order(
                dataframes["df_sales_order"]
            )
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
            df_star_schema = {
                "fact_sales_order": df_fact_sales_order,
                "dim_staff": dim_staff,
                "dim_location": dim_location,
                "dim_design": dim_design,
                "dim_date": dim_date,
                "dim_currency": dim_currency,
                "dim_counterparty": dim_counterparty,
            }
            logger.info("Star-schema data successfully transformed")
            return df_star_schema
    except Exception:
        logger.error(
            "Insuffcient dataframes for star-schema data transformation"
        )
        raise
