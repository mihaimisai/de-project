import pandas as pd
from io import BytesIO
from .list_s3 import logger


def upload_df_to_s3(
    s3_client, df, file_key, logger=logger, bucket="project-test-transform-bucket"
):
    try:
        logger.info(f"Trying to read the file {file_key} from bucket {bucket}")
        # Try to read the existing file from S3
        s3_object = s3_client.get_object(Bucket=bucket, Key=file_key)
        existing_df = pd.read_parquet(BytesIO(s3_object["Body"].read()))
        logger.info(f"Successfully read the existing {file_key} file")

        # Append the new data to the existing DataFrame
        updated_df = pd.concat([existing_df, df], ignore_index=True)
        logger.info("Appended new data to the existing DataFrame")
    except s3_client.exceptions.NoSuchKey:
        # If the file doesn't exist, use the new DataFrame as is
        logger.info("File not found. Using the new DataFrame")
        updated_df = df

    # Write the updated DataFrame back to the S3 bucket
    out_buffer = BytesIO()
    updated_df.to_parquet(out_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key=file_key, Body=out_buffer.getvalue()) # noqa
    logger.info(f"Successfully uploaded the updated DataFrame to {file_key} in bucket {bucket}") # noqa