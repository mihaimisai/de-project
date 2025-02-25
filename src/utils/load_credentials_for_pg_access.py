import os
from dotenv import load_dotenv


def load_credentials_for_pg_access(logger):
    """Load PostgreSQL credentials from .env file."""
    load_dotenv()

    # Retrieve credentials from environment variables
    try:
        PG_HOST = os.getenv("PG_HOST")
        PG_PORT = os.getenv("PG_PORT")
        PG_DATABASE = os.getenv("PG_DATABASE")
        PG_USER = os.getenv("PG_USER")
        PG_PASSWORD = os.getenv("PG_PASSWORD")

        logger.info("Credentials for PostgreSQL access successfully loaded")
        return [PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]
    except Exception as e:
        if not all([PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]):
            logger.error("One or more PostgreSQL credentials are missing.")
            raise ValueError("One or more PostgreSQL credentials are missing.")
        else:
            logger.error(f"Error loading PostgreSQL credentials: {e}")
            raise Exception(e)
