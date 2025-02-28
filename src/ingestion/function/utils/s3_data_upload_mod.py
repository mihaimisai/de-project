from datetime import datetime

def s3_data_upload_mod(
    client,
    bucket_name,
    table_name,
    csv_df,
    logger,
    time_stamp
):

    try:

        # Define the file path in the S3 bucket
        if not time_stamp:
            now = datetime.now()
            time_stamp = now.strftime("%Y-%m-%d %H:%M:%S")                       
            year = now.strftime("%Y")
            month = now.strftime("%m")
            day =now.strftime("%d")
        else:
            year = time_stamp[0:4]
            month = time_stamp[5:7]
            day = time_stamp[8:10]

        s3_key_ingestion = f"{table_name}/{year}/{month}/{day}/{time_stamp}.csv"

        # Upload the file to bucket_name
        client.put_object(
            Bucket=bucket_name, Body=csv_df, Key=s3_key_ingestion
        )

        logger.info(
            f"Successfully uploaded csv file to S3 bucket '{bucket_name}' for table '{table_name}'"  # noqa 501
        )

    except Exception as e:
        logger.error(
            f"Error uploading csv file to S3 for table '{table_name}': {e}"
        )
        raise Exception(e)
