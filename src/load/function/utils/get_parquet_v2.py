import pandas as pd
import io


def get_recent_parquet_files_from_s3(bucket_name, client, logger, num_files=7):
    """
    Retrieves the 7 most recently modified files from
    an S3 bucket and loads them into pandas DataFrames.
    The keys are the table_name
    (extracted from the file path).

    Parameters:
        bucket_name (str):
        client:
        num_files (int):(default is 7).

    Returns:
        dict: A dictionary where keys are table names and
        values are pandas DataFrames.
    """

    try:
        # List all objects in the bucket
        response = client.list_objects_v2(Bucket=bucket_name)

        if "Contents" not in response:
            raise ValueError("Transformed Data Bucket Empty.")

        # Sort files by 'LastModified' timestamp (descending order)
        sorted_files = sorted(
            response["Contents"],
            key=lambda obj: obj["LastModified"],
            reverse=True,
        )

        # Get the most recent `num_files`
        recent_files = sorted_files[:num_files]

        # Dictionary to store {table_name: DataFrame}
        df_dict = {}

        for file in recent_files:
            key = file["Key"]

            # Extract table name
            table_name = key.split("/")[0]

            file_obj = client.get_object(Bucket=bucket_name, Key=key)
            file_body = file_obj["Body"].read()

            df = pd.read_parquet(io.BytesIO(file_body))
            df_dict[table_name] = df

        logger.info("Successfully loaded transformed parquet files.")
        return df_dict

    except ValueError as e:
        logger.error(f"Error loading transformed parquet files: {e}")
        raise e
