from pg8000 import Connection
from .load_credentials_for_data_warehouse import dw_access


def connect_to_db(logger):
    """
    Establishes database connection to the Data Warhouse
    os using credentials.

        Parameters:
            logger (Logger): logger instance

        Logs:
            either info f"Connecting to PostgreSQL database:
            {PG_DATABASE} on host: {PG_HOST}" if successful
            or error f"Connection failed: {e}"

        Returns:
            connection (Connection)

        Raises:
            Exception if connection unsuccessful
    """
    try:
        PG_HOST_DW, PG_PORT_DW, PG_DATABASE_DW, PG_USER_DW, PG_PASSWORD_DW = (
            dw_access()
        )
        # Establish a connection to PostgreSQL database

        connection = Connection(
            host=PG_HOST_DW,
            port=PG_PORT_DW,
            database=PG_DATABASE_DW,
            user=PG_USER_DW,
            password=PG_PASSWORD_DW,
        )

        logger.info(
            f"Connecting to Warehouse: {PG_DATABASE_DW} on host: {PG_HOST_DW}"  # noqa
        )

        return connection

    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise e
