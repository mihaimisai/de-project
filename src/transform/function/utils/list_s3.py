import io
import pandas as pd

from botocore.exceptions import ClientError


def ingested_data_retrival(
    s3_client, files_dict: dict, logger, bucket_name: str
):
    """
    Retrieves CSV files from an S3 bucket and changes data into DataFrames.

    Args:
        s3_client (boto3.client): A boto3 S3 client object.
        files_dict (dict): A dictionary where keys are table names
            and values are S3 file paths.
        logger (logging.Logger): A logger object for logging messages.
        bucket_name (str): The name of the S3 bucket.

    Returns:
        dict: A dictionary where keys are table names and values are
            pandas DataFrames containing the CSV data.
        Returns an empty dictionary if an error occurs or no files are found.
    """
    retrieved_data = {}

    for table_name, file_path in files_dict.items():
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
            csv_content = response["Body"].read().decode("utf-8")
            df = pd.read_csv(io.StringIO(csv_content))
            retrieved_data[table_name] = df
            logger.info(
                f"Successfully retrieved {file_path} for table {table_name}"
            )

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.error(f"File not found: {file_path}.")
            else:
                logger.error(
                    f"Error retrieving {file_path} for table {table_name}."
                )

        except Exception as e:
            logger.error(
                f"Unexpected error retrieving {file_path} for table {table_name}: {e}" # noqa 501
            )

    return retrieved_data
