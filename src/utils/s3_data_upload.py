import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def s3_data_upload(client, bucket_name, table_name, buffer):
    try:

        # Define the file path in the S3 bucket

        s3_key_ingestion = f"ingestion/{table_name}.parquet"

        # Upload the Parquet file to bucket_name
        client.upload_fileobj(buffer, bucket_name, s3_key_ingestion)
        logger.info(
            f"Successfully uploaded Parquet file to S3 bucket '{bucket_name}' for table '{table_name}'"  # noqa
        )
    except Exception as e:
        logger.error(
            f"Error uploading Parquet file to S3 for table '{table_name}': {e}"
        )
        raise
