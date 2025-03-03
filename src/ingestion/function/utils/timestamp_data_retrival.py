def timestamp_data_retrival(client, s3_timestamp_bucket, table_name, logger):
    '''
    Retrieves timestamp from txt file in s3 timestamp bucket. 

        Parameters:
            client (): s3 client
            s3_timestamp_bucket (str): name of s3 bucket to retrieve from
            table_name (str): table to retrieve timestamp of last query for - referenced in txt file name
            logger (Logger): logger instance

        Logs:
            either info f"Successfully retrieved time_stamp_{table_name}.txt file from S3 bucket '{s3_timestamp_bucket}'" if successful
            or error f"Error retrieving time_stamp_{table_name}.txt from S3 bucket: '{s3_timestamp_bucket}': {e}"

        Returns:
            either time_stamp (format "%Y-%m-%d %H:%M:%S") if successful
            or None if 'NoSuchKey' error - if no timestamp file (first lambda invocation) or incorrect table_name
            
        Raises:
            Exception if retireval unsuccessful unless due to 'NoSuchKey'
    '''
    try:
        time_stamp = client.get_object(
            Bucket=s3_timestamp_bucket, Key=f"time_stamp_{table_name}.txt"
        )
        content = time_stamp["Body"].read()
        time_stamp = content.decode("utf-8")
        logger.info(
            f"Successfully retrieved time_stamp_{table_name}.txt file from S3 bucket '{s3_timestamp_bucket}'"  # noqa
        )
        return time_stamp
    except Exception as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        else:
            logger.error(
                f"Error retrieving time_stamp_{table_name}.txt from S3 bucket: '{s3_timestamp_bucket}': {e}"  # noqa
            )
            raise e