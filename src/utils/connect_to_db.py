from pg8000 import Connection
from src.utils.load_credentials_for_pg_access import pg_access


def connect_to_db(logger):
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
    conn.close()
