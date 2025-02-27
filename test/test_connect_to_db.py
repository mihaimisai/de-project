from src.utils.connect_to_db import connect_to_db
import pytest
import logging
from unittest.mock import patch
from pg8000.exceptions import InterfaceError
from testfixtures import LogCapture


@pytest.fixture
def test_logger():
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.INFO)
    return logger


class TestConnectToDb:
    @patch(
        "src.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    def test_fails_with_no_connection_established(self, test_logger):
        with pytest.raises(InterfaceError):
            connect_to_db(test_logger)

    @patch(
        "src.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "anyyy"),
    )
    @patch("src.utils.connect_to_db.Connection", return_value=object())
    def test_connects_to_db_success(
        self, mock_access, mock_connection, test_logger
    ):
        connect_to_db(test_logger)

        mock_access.assert_called_once_with(
            "localhost", 1234, "test_db", "user", "anyyy"
        )

    def test_connects_to_db_logs_error(self, test_logger):
        with LogCapture(level=logging.ERROR) as logstream:
            try:
                connect_to_db(test_logger)
            except Exception:
                test_logger.info("Error in connection in test file")
        expected_log = (
            "test_logger",
            "ERROR",
            "Connection failed: One or more PostgreSQL credentials are missing.",  # noqa
        )
        assert logstream[0] == expected_log

    @patch(
        "src.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch("src.utils.connect_to_db.Connection", return_value=object())
    def test_connects_to_db_logs_info(
        self, mock_access, mock_connection, test_logger
    ):
        with LogCapture(level=logging.INFO) as logstream:
            try:
                connect_to_db(test_logger)
            except Exception:
                test_logger.ERROR("Error in connection in test file")

        assert logstream[0] == (
            "test_logger",
            "INFO",
            "Connecting to PostgreSQL database: test_db on host: localhost",  # noqa
        )
