import io
import pandas as pd
import logging
from datetime import datetime, timedelta
# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
temp_bucket = "de-project-ingested-data-20250227143401632000000004"

def ingested_data_retrival(s3_client,
                           ingested_bucket_name,
                           logger=logger):
    """
    Retrieve and process ingested data from an S3 bucket.

    Args:
        s3_client (boto3.client):
        The S3 client to use for accessing the bucket.
        ingested_bucket_name (str):
        The name of the S3 bucket containing the ingested data.
        logger (logging.Logger):
        The logger instance for logging information and errors.

    Returns:
        tuple: A tuple containing two elements:
            - dataframes (dict):
            A dictionary of Pandas DataFrames,
            where keys are table names prefixed with 'df_'.
            - dataframes_info (dict):
            A dictionary containing timestamp information and table names.
    """
    try:
        # List objects in the bucket
        objects = []
        response = s3_client.list_objects_v2(Bucket=ingested_bucket_name)
        
        while response['IsTruncated']:
            objects.extend(response['Contents'])  
            response = s3_client.list_objects_v2(
                Bucket=ingested_bucket_name, 
                ContinuationToken=response['NextContinuationToken']
            )
        # Adding the last set of objects
        objects.extend(response['Contents'])


        logger.info(f"Successfully listed objects from bucket:{ingested_bucket_name}") # noqa
    except Exception as e:
        logger.error(f"Error: bucket {ingested_bucket_name} does not exist: {e}") # noqa
        raise Exception(f"Error: bucket {ingested_bucket_name} does not exist: {e}") # noqa

    # Ensure we actually have a valid response
    if "Contents" not in response:
        logger.error(f"The bucket:{ingested_bucket_name} is empty")
        # Raise an exception to stop further execution
        raise ValueError(f"The bucket:{ingested_bucket_name} is empty")

    # Extract keys from the response
    keys = [obj["Key"] for obj in objects]

    tables_keys = [key.split('/')[0] for key in keys]
    table_names = list(set(tables_keys))

    # Extract the timestamp portion from each key
    timestamps = [key[-23:-4] for key in keys]

    # Convert strings to datetime objects
    dt_objects = [datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") for ts in timestamps]

    # Find the latest timestamp
    latest_time = max(dt_objects)

    # Define the 2-minute threshold
    delta = [(time-latest_time).total_seconds() for time in dt_objects]

    # Get indexes of timestamps within the last 2 minutes
    indexes = [i for i, dt in enumerate(delta) if dt >= -120]

    
    # Find the maximum (latest) timestamp

    year = str(latest_time)[0:4]
    month = str(latest_time)[5:7]
    day = str(latest_time)[8:10]
    hour = str(latest_time)[11:13]
    minute = str(latest_time)[14:16]


    # Filter keys that start with the latest timestamp
    latest_keys = []
    for i in list(range(len(indexes))):
        latest_keys.append(keys[indexes[i]])
    # [key for key in keys if key.endswith(latest_time+'.csv')]
    
    
    
    # Define dataframes info
    dataframes_info = {
        "timestamp": {
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
            "minute": minute,
        },
        "table_names": table_names,
    }
    # define an empty dictionary for dataframes
    dataframes = {}

    for i in range(len(latest_keys)):
        # Retrieve the object
        obj_response = s3_client.get_object(
            Bucket=ingested_bucket_name, Key=latest_keys[i]
        )
        body_content = obj_response["Body"].read()
        dataframes[f"df_{table_names[i]}"] = pd.read_csv(io.BytesIO(body_content)) # noqa

    logger.info(f"Successfully converted CSV tables from bucket:{ingested_bucket_name} in pandas dataframe") # noqa

    return dataframes, dataframes_info


from .s3_client import s3_client
from pprint import pprint as pp
s3_client = s3_client()
# Specify the bucket name
ingested_bucket_name = "de-project-ingested-data-20250227143401632000000004"


dataframes, dataframes_info = ingested_data_retrival(
                                                    s3_client,
                                                    ingested_bucket_name,
                                                    logger=logger)
print("<<dataframes>>")
pp(dataframes)
print("<<dataframes_info>>")
pp(dataframes_info)
