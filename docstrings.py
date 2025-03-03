# # ingestion_handler
# '''
# Orchestrates ingestion and storage of data from multiple tables in database. 

# Invokes s3_client and process_all_tables.

#     Parameters:
#         event (event object): json object - converted to empty dict by lambda python runtime
#         context (context object): context object provided by AWS

#     Returns:
#         dictionary, {'statusCode': 200, "body": "Data ingestion complete" } if successful
# '''

# # upload_time_stamp
# '''
# Uploads txt file containing formatted current timestamp to s3 timestamp bucket. 

    # Parameters:
    #     client (): s3 client
    #     bucket_name (str): name of s3 bucket to upload to
    #     table_name (str): table that has been queried at this timestamp
    #     logger (Logger): logger instance
    #     time_stamp (str): timestamp to upload

    # Logs:
    #     either info f"Successfully uploaded {formatted_now}_{table_name}.txt file to S3 bucket '{bucket_name}'" if successful
    #         where formatted_now is a timestamp with format "%Y-%m-%d %H:%M:%S"
    #     or error f"Error uploading time_stamp_{table_name}.txt to S3 bucket: '{bucket_name}': {e}"

    # Raises:
    #     Exception if upload unsuccessful
# '''

# # timestamp_data_retrival
# '''
# Retrieves timestamp from txt file in s3 timestamp bucket. 

#     Parameters:
#         client (): s3 client
#         s3_timestamp_bucket (str): name of s3 bucket to retrieve from
#         table_name (str): table to retrieve timestamp of last query for - referenced in txt file name
#         logger (Logger): logger instance

#     Logs:
#         either info f"Successfully retrieved time_stamp_{table_name}.txt file from S3 bucket '{s3_timestamp_bucket}'" if successful
#         or error f"Error retrieving time_stamp_{table_name}.txt from S3 bucket: '{s3_timestamp_bucket}': {e}"

#     Returns:
#         either time_stamp (format "%Y-%m-%d %H:%M:%S") if successful
#         or None if 'NoSuchKey' error - if no timestamp file (first lambda invocation) or incorrect table_name
        
#     Raises:
#         Exception if retireval unsuccessful unless due to 'NoSuchKey'
# '''

# # s3_data_upload
# '''
# Uploads csv file containing ingested data to s3 ingested data bucket.  

#     Parameters:
#         client (): s3 client
#         bucket_name (str): name of s3 bucket to upload to
#         table_name (str): table data has been retrieved from - referenced in s3 file name
#         csv_df (csv): csv file to upload
#         logger (Logger): logger instance
 
#     Logs:
#         either info f"Successfully uploaded csv file to S3 bucket '{bucket_name}' for table '{table_name}'" if successful
#         or error f"Error uploading csv file to S3 for table '{table_name}': {e}"

#     Returns:
#         time_stamp (str): time of upload in format "%Y-%m-%d %H:%M:%S"
       
#     Raises:
#         Exception if upload unsuccessful
# '''

# # s3_client
# '''
# Initializes s3 client.

#     Parameters:
#         region (str): AWS region (default is 'eu-west-2')

#     Returns:
#         s3 client
# '''

# process_all_tables
# why reassigning client when passes in as argument?
# expand comments describing accessing bucket names from lambda environment variables and configuring logger 
'''
Orchestrates ingestion and processing of multiple tables.

Invokes ingest_data_to_s3 function for list of tables.

    Parameters:
        client (): s3 client
        logger (Logger): logger instance (default is logger - variable defined in enclosing scope)
'''

# load_credentials_for_pg_access - pg_access
'''
Retrieves database credentials from lambda environment variables. 

    Returns:
        list of database credential variables
        [PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]
        
    Raises:
        ValueError("One or more PostgreSQL credentials are missing.") if unsuccessful
'''

# connect_to_db - connect_to_db
'''
Establishes database connection using credentials. 

    Parameters:
        logger (Logger): logger instance

    Logs:
        either info f"Connecting to PostgreSQL database: {PG_DATABASE} on host: {PG_HOST}" if successful
        or error f"Connection failed: {e}"

    Returns:
        connection (Connection)
        
    Raises:
        Exception if connection unsuccessful
'''

# connect_to_db - close_db
'''
Closes database connection.

    Parameters:
        conn (Connection): database connection
'''

# ingest_data_to_s3 - fetch_data
'''
Fetches new data from table in a database using psql queries.

Only fetches data added to the table since last data retrival.

    Parameters:
        conn (Connection): database connection
        table_name (str): table to retrieve data from
        logger (Logger): logger instance
        time_stamp (str): timestamp of last data retrival from table

    Logs:
        error f"Error fetching data: {e}") if unsuccessful

    Returns:
        dataframe (DataFrame): empty if error fetching data
 
    Raises:
        ValueError("Invalid table name") if invalid table_name
'''

# ingest_data_to_s3 - convert_to_csv
'''
Converts pandas dataframe to csv file.

Csv file does not include row names (index).

    Parameters:
        df (DataFrame): pandas dataframe to convert

    Returns:
        csv format (str)
'''

# ingest_data_to_s3 - ingest_data_to_s3
'''
Orchestrates retrival, conversion and upload of data from a database table to a csv file in an s3 bucket.

    Parameters:
        s3_client (): s3 client
        logger (Logger): logger instance
        table_name (str): table to retrieve data from
        s3_ingestion_bucket (str): name of s3 bucket to upload csv to
        s3_timestamp_bucket (str): name of s3 bucket to retrieve and upload timestamp to
    
    Logs:
        either info f"Successfully fetched data from table: {table_name}"
        or error f"Error connecting to PostgreSQL or executing query for table '{table_name}': {e}"
    
    Raises:
        Exception if unsuccessful
'''

# generate timestamp - carlo refactor?

# code package - list modules and dependencies?
# boto3, datetime, logging, os, 
# pg8000, pandas