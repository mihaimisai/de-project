import os
from datetime import datetime
  

def upload_df_to_s3(
    s3_client,
    df,
    file_key,
    logger,
    transform_bucket_name,
):
    """
    Uploads a DataFrame to an S3 bucket in Parquet format.
    Args:
        s3_client (boto3.client):
        The S3 client used to upload the file.
        df (pandas.DataFrame):
        The DataFrame to be uploaded.
        file_key (str):
        The key (path) for the file to be uploaded.
        logger (logging.Logger):
        The logger for logging messages.
        transform_bucket_name (str):
        The name of the S3 bucket where the file will be uploaded.
    Raises:
        Exception:
        If the DataFrame cannot be converted to Parquet
        or if the file cannot be uploaded to S3.
    """
    now = datetime.now()

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    time_stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    s3_key_transform = f"{file_key}/{year}/{month}/{day}/{time_stamp}.parquet"

    # Try to convert DataFrame to Parquet
    try:
        # current_directory = os.getcwd()
        file_key_path = '/tmp/'+file_key
        df.to_parquet(file_key_path, engine="pyarrow")
    except Exception as e:
        logger.error(
            f"Failed to convert DataFrame to Parquet for {file_key}: {e}"
        )  # noqa
        raise  # Stop execution and propagate the error

    # Try to upload file to S3
    try:
        s3_client.upload_file(file_key_path, transform_bucket_name, s3_key_transform)  # noqa
    except Exception as e:
        logger.error(
            f"Failed to upload file {file_key} to S3 bucket {transform_bucket_name}: {e}"  # noqa
        )
        raise  # Stop execution and propagate the error

    logger.info(
        f"Successfully uploaded DataFrame to {file_key} in bucket {transform_bucket_name}"  # noqa
    )

    # Try to remove the local file after upload
    try:
        os.remove(file_key_path)
        logger.info(f"Successfully removed local copy of {file_key}")
    except Exception as e:
        logger.warning(
            f"File {file_key} could not be removed after upload: {e}"
        )  # noqa
