from datetime import datetime


def upload_time_stamp(client, bucket_name, table_name, logger, time_stamp):
    try:

        # Define the S3 key for the timestamp file
        s3_key_ingestion = f"time_stamp_{table_name}.txt"

        # Upload the timestamp file to S3
        client.put_object(
            Bucket=bucket_name, Body=time_stamp, Key=s3_key_ingestion
        )
        logger.info(
            f"Successfully uploaded time_stamp_{table_name}.txt file to S3 bucket '{bucket_name}'"  # noqa
        )

    except Exception as e:
        logger.error(
            f"Error uploading time_stamp_{table_name}.txt to S3 bucket: '{bucket_name}': {e}"  # noqa
        )
        raise Exception(e)
