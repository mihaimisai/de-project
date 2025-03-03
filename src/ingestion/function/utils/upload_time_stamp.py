
def upload_time_stamp(client, bucket_name, table_name, logger, time_stamp):
    '''
    Uploads txt file containing formatted current timestamp to s3 timestamp bucket. 

        Parameters:
            client (): s3 client
            bucket_name (str): name of s3 bucket to upload to
            table_name (str): table that has been queried at this timestamp
            logger (Logger): logger instance
            time_stamp (str): timestamp to upload

        Logs:
            either info f"Successfully uploaded {formatted_now}_{table_name}.txt file to S3 bucket '{bucket_name}'" if successful
                where formatted_now is a timestamp with format "%Y-%m-%d %H:%M:%S"
            or error f"Error uploading time_stamp_{table_name}.txt to S3 bucket: '{bucket_name}': {e}"

        Raises:
            Exception if upload unsuccessful
    '''
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
