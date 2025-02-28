def timestamp_data_retrival(client, s3_timestamp_bucket, table_name, logger):
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
        print(e.response)
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        else:
            logger.error(
                f"Error retrieving time_stamp_{table_name}.txt from S3 bucket: '{s3_timestamp_bucket}': {e}"  # noqa
            )
            raise e
