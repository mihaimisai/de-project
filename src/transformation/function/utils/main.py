from .s3_client import s3_client
from .list_s3 import logger
from .star_schema import star_schema
from .parquet_upload import upload_df_to_s3


# Initialize the S3 client
client = s3_client()


def main(client, logger=logger, bucket="project-test-transform-bucket"):
    df_star_schema = star_schema(client, logger=logger)
    star_schema_table_names = list(df_star_schema.keys())
    try:
        [
            upload_df_to_s3(
                client,
                df_star_schema[key],
                key+".parquet",
                logger=logger,
                bucket="project-test-transform-bucket",
            )
            for key in star_schema_table_names
        ]
        logger.info(f"Successfully uploaded data to {bucket}")
    except Exception as e:
        logger.error(f"Failed to upload data to {bucket}: {e}")
        raise