from .utils.get_latest_files import get_latest_files
from .utils.star_schema import star_schema
from .utils.s3_client import s3_client
from .utils.parquet_upload import upload_df_to_s3
import logging
import os


def transform_handler(event, context):
    """
    Is invoked when triggered by an event

    Invokes s3_client, get_latest_files, star_schema and upload_df_to_s3

        Parameters:
            event (event object): json object - converted to empty dict
            by lambda python runtime
            context (context object): context object provided by AWS

        Returns:

            Connects to the s3 clent, fetch the latest files uploaded to
            ingested_bucke_name, this dictionary is passed to df_star_schema
            then this dictionary is passed to upload_df_to_s3

            {'statusCode': 200, "body": "Data transformation complete" }
            if successful
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    client = s3_client()
    ingested_data_bucket = os.environ.get("ingested_data_bucket")
    transformed_data_bucket = os.environ.get("transformed_data_bucket")

    files_dict = get_latest_files(client, ingested_data_bucket, logger)

    df_star_schema = star_schema(
        client, ingested_data_bucket, logger, files_dict
    )  # noqa

    star_schema_table_names = list(df_star_schema.keys())
    try:
        [
            upload_df_to_s3(
                client,
                df_star_schema[key],
                key,
                logger,
                transformed_data_bucket,
            )
            for key in star_schema_table_names
        ]
        logger.info(f"Successfully uploaded data to {transformed_data_bucket}")
    except Exception as e:
        logger.error(
            f"Failed to upload data to {transformed_data_bucket}: {e}"
        )
        raise

    return {"statusCode": 200, "body": "Data transformation complete"}
