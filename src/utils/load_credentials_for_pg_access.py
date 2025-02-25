import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_credentials_for_pg_access():
    """Load PostgreSQL credentials from .env file."""
    load_dotenv()

    # Retrieve credentials from environment variables
    PG_HOST = os.getenv("PG_HOST")
    PG_PORT = os.getenv("PG_PORT")
    PG_DATABASE = os.getenv("PG_DATABASE")
    PG_USER = os.getenv("PG_USER")
    PG_PASSWORD = os.getenv("PG_PASSWORD")

    logger.info("Credentials for PostgreSQL access successfully loaded")
    return [PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]
