from pg8000 import Connection
from src.utils.load_credentials_for_pg_access import (
    load_credentials_for_pg_access,
)


def connect_to_db(logger):
    # Load database credentials
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD = (
        load_credentials_for_pg_access(logger)
    )

    try:
        # Establish a connection to PostgreSQL database
        logger.info(
            f"Connecting to PostgreSQL database: {PG_DATABASE} on host: {PG_HOST}"  # noqa
        )
        return Connection(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
        )

    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise Exception(e)


def close_db(conn):
    conn.close()
