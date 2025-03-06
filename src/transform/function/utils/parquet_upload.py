import os
from datetime import datetime

def upload_df_to_s3(
    s3_client,
    df,
    file_key,
    logger,
    transform_bucket_name,
):
    now = datetime.now()

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    time_stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    s3_key_ingestion = (
        f"{file_key}/{year}/{month}/{day}/{time_stamp}.parquet"
    )

    # Try to convert DataFrame to Parquet
    try:
        df.to_parquet(file_key, engine='pyarrow')
    except Exception as e:
        logger.error(f"Failed to convert DataFrame to Parquet for {file_key}: {e}")
        raise  # Stop execution and propagate the error

    # Try to upload file to S3
    try:
        s3_client.upload_file(file_key, transform_bucket_name, s3_key_ingestion)
    except Exception as e:
        logger.error(f"Failed to upload file {file_key} to S3 bucket {transform_bucket_name}: {e}")
        raise  # Stop execution and propagate the error

    logger.info(
        f"Successfully uploaded DataFrame to {file_key} in bucket {transform_bucket_name}"
    )

    # Try to remove the local file after upload
    try:
        os.remove(file_key)
        logger.info(
        f"Successfully removed local copy of {file_key}"
    )
    except Exception as e:
        logger.warning(f"File {file_key} could not be removed after upload: {e}")
