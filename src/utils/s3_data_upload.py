def s3_data_upload(client, bucket_name, table_name, csv_df, logger, time_stamp):
    try:

        # Define the file path in the S3 bucket

        s3_key_ingestion = f"{table_name}/{time_stamp}.csv"

        # Upload the Parquet file to bucket_name
        client.put_object(Bucket=bucket_name, Body=csv_df, Key=s3_key_ingestion)

        logger.info(
            f"Successfully uploaded Parquet file to S3 bucket '{bucket_name}' for table '{table_name}'"  # noqa
        )

    except Exception as e:
        logger.error(
            f"Error uploading Parquet file to S3 for table '{table_name}': {e}"
        )
        raise Exception(e)
