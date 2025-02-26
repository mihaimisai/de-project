from pg8000.native import Connection
from pg8000.exceptions import InterfaceError, DatabaseError
from .load_credentials_for_pg_access import (
    pg_access,
)


def connect_to_db(logger):
    # Load database credentials
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD = (
        pg_access()
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

    except InterfaceError as e:
        logger.error(f"Connection failed: {e}")
        raise e
        


def close_db(conn):
    conn.close()
