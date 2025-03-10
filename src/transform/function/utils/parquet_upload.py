from io import BytesIO
from datetime import datetime


def upload_df_to_s3(
    s3_client,
    df,
    file_key,
    logger,
    transform_bucket_name,
):
    """
    Uploads a DataFrame to an S3 bucket in Parquet format
    using an in-memory buffer.
    Args:
        s3_client (boto3.client): The S3 client used to upload the file.
        df (pandas.DataFrame): The DataFrame to be uploaded.
        file_key (str): The key (path) for the file.
        logger (logging.Logger): The logger for logging messages.
        transform_bucket_name (str): The name of the S3 bucket.
    Raises:
        Exception: If conversion to Parquet or S3 upload fails.
    """
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    time_stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    s3_key_transform = f"{file_key}/{year}/{month}/{day}/{time_stamp}.parquet"

    # Write DataFrame to an in-memory buffer in Parquet format.
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, engine="pyarrow")
        buffer.seek(0)  # Reset the pointer to the beginning of the buffer
    except Exception as e:
        logger.error(
            f"Failed to convert DataFrame to Parquet for {file_key}: {e}"
        )  # noqa
        raise e

    # Upload the in-memory buffer to S3.
    try:
        s3_client.upload_fileobj(
            buffer, transform_bucket_name, s3_key_transform
        )  # noqa
    except Exception as e:
        logger.error(
            f"Failed to upload file {file_key} to S3 bucket {transform_bucket_name}: {e}"  # noqa
        )
        raise e

    logger.info(
        f"Successfully uploaded DataFrame to {file_key} in bucket {transform_bucket_name}"  # noqa
    )
