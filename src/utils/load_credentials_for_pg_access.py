import os

def pg_access(logger):


    # Retrieve credentials from environment variables
    try:
        PG_HOST = os.environ("DB_HOST")
        PG_PORT = os.environ("DB_PORT")
        PG_DATABASE = os.environ("DB")
        PG_USER = os.environ("DB_USER")
        PG_PASSWORD = os.environ("DB_PASSWORD")


        return [PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]
    except Exception as e:
        if not all([PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]):
            raise ValueError("One or more PostgreSQL credentials are missing.")
        else:
            raise Exception(e)
