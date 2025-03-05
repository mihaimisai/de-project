import pandas as pd

def insert_dataframe_in_db(conn, table_name, df, logger):
    """
    Inserts a pandas DataFrame into a PostgreSQL table row by row.

    Args:
        conn (pg8000.Connection): The database connection object.
        table_name (str): The name of the table to insert data into.
        df (pd.DataFrame): The pandas DataFrame containing the data to insert.
        logger (logging.Logger): A logger object for logging messages.

    Raises:
        Exception: If an error occurs during the insertion process, the transaction is rolled back,
        and the exception is re-raised.
    """
    cur = conn.cursor()
    try:
        for index, row in df.iterrows():
            sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
            cur.execute(sql, tuple(row))
        conn.commit()
        logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        logger.error(f"General Error inserting data into {table_name}: {e}")
        raise 
    cur.close() 
