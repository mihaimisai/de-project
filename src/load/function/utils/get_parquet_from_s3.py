import pandas as pd
import io


def table_list():
    """Returns a list of the
    7 parquet file names.

    Utilised for Patching."""

    key_list = [
        "fact_sales_order",
        "dim_staff",
        "dim_location",
        "dim_design",
        "dim_date",
        "dim_currency",
        "dim_counterparty",
    ]
    return key_list


def get_parquet_from_s3(logger, client, s3_processed_bucket):
    """
    Collects parquet files from s3 Processed Data Bucket
    and places data into df_dict.

    Parameters:
        Logger,
        s3 Client,
        s3_processed_bucket_name

    Returns:
        df_dict where key=table_name and value=parquet_data
    """

    try:
        df_dict = {}
        for key in table_list():
            response = client.get_object(
                Bucket=s3_processed_bucket, Key=f"{key}"
            )
            parquet_data = response["Body"].read()
            df = pd.read_parquet(io.BytesIO(parquet_data))
            df_dict[key] = df

        logger.info("Parquet data successfully retrieved within load lambda.")
        return df_dict
    except Exception as e:
        logger.error(
            f"Error loading in the parquet data within Load Lambda: {e}"
        )  # noqa
        raise e
