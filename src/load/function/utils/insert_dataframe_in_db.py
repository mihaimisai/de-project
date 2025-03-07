from pg8000.native import identifier


def insert_dataframe_in_db(conn, table_name, df, logger):
    """
    Inserts a pandas DataFrame into a PostgreSQL table row by row.

    Args:
        conn (pg8000.Connection): The database connection object.
        table_name (str): The name of the table to insert data into.
        df (pd.DataFrame): The pandas DataFrame containing the data to insert.
        logger (logging.Logger): A logger object for logging messages.

    Raises:
        Exception:
            If an error the transaction is rolled back,
        and the exception is re-raised.
    """
    cur = conn.cursor()
    try:
        if table_name == "dim_date":
            cur.execute("SELECT * FROM dim_date LIMIT 1")
            result = cur.fetchone()
            logger.debug(result)
            if result:
                logger.info("dim_date already has data. Skipping insertion.")
                return
        for index, row in df.iterrows():
            columns_str = ", ".join(df.columns)
            placeholders_str = ", ".join(["%s"] * len(df.columns))

            query = "INSERT INTO "
            query += f"{identifier(table_name)} ({columns_str}) "
            query += "VALUES "
            query += f"({placeholders_str})"
            cur.execute(query, tuple(row))
        conn.commit()
        logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        logger.error(f"General Error inserting data into {table_name}: {e}")
        raise
    cur.close()
