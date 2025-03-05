import os


def dw_access():
    """
    Retrieves database Data-Warehouse credentials
    from lambda environment variables.

        Returns:
            list of database credential variables
            [PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]

        Raises:
            ValueError("One or more PostgreSQL credentials
            are missing.") if unsuccessful
    """
    # Retrieve credentials from environment variables
    PG_HOST = os.environ.get("DB_HOST_DW")
    PG_PORT = os.environ.get("DB_PORT_DW")
    PG_DATABASE = os.environ.get("DB_DW")
    PG_USER = os.environ.get("DB_USER_DW")
    PG_PASSWORD = os.environ.get("DB_PASSWORD_DW")

    if not all([PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]):
        raise ValueError(
            "Missing one or more PostgreSQL credentials for the Warehouse."
        )  # noqa

    return [PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]
