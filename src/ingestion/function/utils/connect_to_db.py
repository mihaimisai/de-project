from pg8000 import Connection
from .load_credentials_for_pg_access import pg_access


def connect_to_db(logger):
    """
    Establishes database connection using credentials.

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
        PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD = pg_access()
        # Establish a connection to PostgreSQL database

        connection = Connection(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
        )

        logger.info(
            f"Connecting to PostgreSQL database: {PG_DATABASE} on host: {PG_HOST}"  # noqa
        )

        return connection

    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise e


def close_db(conn):
    """
    Closes database connection.

        Parameters:
            conn (Connection): database connection
    """
    conn.close()
