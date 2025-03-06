from .utils.get_latest_files import get_latest_files
from .utils.star_schema import star_schema
from .utils.s3_client import s3_client
from .utils.parquet_upload import upload_df_to_s3
import logging
import os


def transform_handler(event, context):
    """
    Is invoked when triggered by an event

    Invokes s3_client, files_dict and ingested_data_retrival.

        Parameters:
            event (event object): json object - converted to empty dict
            by lambda python runtime
            context (context object): context object provided by AWS

        Returns:
            {'statusCode': 200, "body": "Data transformation complete" }
            if successful
    """

    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    client = s3_client()
    ingested_bucket_name = os.environ.get("ingested_data_bucket")
    transform_bucket_name = os.environ.get("processed_data_bucket")

    files_dict = get_latest_files(client, ingested_bucket_name, logger)

    df_star_schema = star_schema(
        client, ingested_bucket_name, logger, files_dict
    )

    star_schema_table_names = list(df_star_schema.keys())
    try:
        [
            upload_df_to_s3(
                client,
                df_star_schema[key],
                key,
                logger,
                transform_bucket_name,
            )
            for key in star_schema_table_names
        ]
        logger.info(f"Successfully uploaded data to {transform_bucket_name}")
    except Exception as e:
        logger.error(f"Failed to upload data to {transform_bucket_name}: {e}")
        raise

    return {"statusCode": 200, "body": "Data transformation complete"}
