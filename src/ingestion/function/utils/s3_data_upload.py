from datetime import datetime


def s3_data_upload(
    client, bucket_name, table_name, csv_df, logger
):
    '''
    Uploads csv file containing ingested data to s3 ingested data bucket.  

        Parameters:
            client (): s3 client
            bucket_name (str): name of s3 bucket to upload to
            table_name (str): table data has been retrieved from - referenced in s3 file name
            csv_df (csv): csv file to upload
            logger (Logger): logger instance

        Logs:
            either info f"Successfully uploaded csv file to S3 bucket '{bucket_name}' for table '{table_name}'" if successful
            or error f"Error uploading csv file to S3 for table '{table_name}': {e}"

        Returns:
            time_stamp (str): time of upload in format "%Y-%m-%d %H:%M:%S"
            
        Raises:
            Exception if upload unsuccessful
    '''
    try:

        now = datetime.now()
        
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day =now.strftime("%d")
        time_stamp = now.strftime("%Y-%m-%d %H:%M:%S")
        
        s3_key_ingestion = f"{table_name}/{year}/{month}/{day}/{time_stamp}.csv"

        # Upload the file to bucket_name
        client.put_object(
            Bucket=bucket_name, Body=csv_df, Key=s3_key_ingestion
        )

        logger.info(
            f"Successfully uploaded csv file to S3 bucket '{bucket_name}' for table '{table_name}'"  # noqa 501
        )
        
        return time_stamp

    except Exception as e:
        logger.error(
            f"Error uploading csv file to S3 for table '{table_name}': {e}"
        )
        raise Exception(e)
