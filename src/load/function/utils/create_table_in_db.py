import logging

def create_table_in_db(conn, table_name: str, table_columns: str, logger: logging.Logger):
    """
    Creates a table in the database if it does not already exist.

    Args:
        conn: The database connection object.
        table_name: The name of the table to create.
        table_columns: A string defining the table columns and their data types.
                       Example: "id SERIAL PRIMARY KEY, name VARCHAR(255), age INTEGER"
        logger: A logger object for logging messages.

    Raises:
        Exception: If an error occurs during the table creation, the transaction is rolled back,
                   and the exception is re-raised.
    """
    cur = conn.cursor()
    try:
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({table_columns})"
        cur.execute(query)
        conn.commit()
        logger.info(f"Successfully created table {table_name}")
    except Exception as e:
        conn.rollback()
        logger.error(f"General Error creating table {table_name}: {e}")
        raise
    finally:
        cur.close()