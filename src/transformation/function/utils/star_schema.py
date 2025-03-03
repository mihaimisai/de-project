from .s3_client import s3_client
from .list_s3 import ingested_data_retrival, logger
from .transform import (
    transform_fact_sales_order,
    transform_dim_staff,
    transform_dim_location,
    transform_dim_design,
    transform_dim_date,
    transform_dim_currency,
    transform_dim_counterparty,
)

# s3 client
client = s3_client()


def star_schema(client,
                ingested_bucket_name="cd-test-ingestion-bucket",
                logger=logger):
    required_dataframes = [
        "df_sales_order",
        "df_staff",
        "df_department",
        "df_address",
        "df_design",
        "df_currency",
        "df_counterparty",
    ]
    dataframes, dataframes_info = ingested_data_retrival(
        client, ingested_bucket_name, logger=logger
    )

    available_dataframes = ["df_" + table for table in dataframes_info["table_names"]] # noqa
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
            star_schema = {
                "fact_sales_order": df_fact_sales_order,
                "dim_staff": dim_staff,
                "dim_location": dim_location,
                "dim_design": dim_design,
                "dim_date": dim_date,
                "dim_currency": dim_currency,
                "dim_counterparty": dim_counterparty,
            }
            logger.info("Star-schema data successfully processed")
            return star_schema
    except Exception:
        logger.error("Insuffcient dataframes for star-schema data processing")
        raise


# star_schema = star_schema(client, ingested_bucket_name, logger=logger)
# print(star_schema)
