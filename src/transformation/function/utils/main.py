import os
from .s3_client import s3_client
from .list_s3 import logger
from .star_schema import star_schema
from .parquet_upload import upload_df_to_s3


# Initialize the S3 client
client = s3_client()

# S3 Configuration
# names obtained from tf
ingested_bucket_name = os.environ.get("ingested_bucket_name")
transform_bucket_name = os.environ.get("transform_bucket_name")

def main(client,
         logger=logger,
         ingested_bucket_name = "cd-test-ingestion-bucket ",
         transform_bucket_name="project-test-transform-bucket"):
    
    df_star_schema = star_schema(client,
                                 ingested_bucket_name,
                                 logger=logger)
    
    star_schema_table_names = list(df_star_schema.keys())
    try:
        [
            upload_df_to_s3(
                client,
                df_star_schema[key],
                key+".parquet",
                logger=logger,
                transform_bucket_name="project-test-transform-bucket",
            )
            for key in star_schema_table_names
        ]
        logger.info(f"Successfully uploaded data to {transform_bucket_name}")
    except Exception as e:
        logger.error(f"Failed to upload data to {transform_bucket_name}: {e}")
        raise

main(client,
    logger=logger,
    ingested_bucket_name = ingested_bucket_name,
    transform_bucket_name = transform_bucket_name)