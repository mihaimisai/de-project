import os

"""
Retrieves database credentials from lambda environment variables.

    Returns:
        list of database credential variables
        [PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]

    Raises:
        ValueError("One or more PostgreSQL credentials
        are missing.") if unsuccessful
"""


def pg_access():

    # Retrieve credentials from environment variables
    PG_HOST = os.environ.get("DB_HOST")
    PG_PORT = os.environ.get("DB_PORT")
    PG_DATABASE = os.environ.get("DB")
    PG_USER = os.environ.get("DB_USER")
    PG_PASSWORD = os.environ.get("DB_PASSWORD")

    if not all([PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]):
        raise ValueError("One or more PostgreSQL credentials are missing.")

    return [PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]
