from src.load.function.utils.connect_to_dw import connect_to_db
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


class TestConnectToDw:
    @patch(
        "src.load.function.utils.connect_to_dw.dw_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    def test_fails_with_no_connection_established(self, test_logger):
        with pytest.raises(InterfaceError):
            connect_to_db(test_logger)

    @patch(
        "src.load.function.utils.connect_to_dw.dw_access",
        return_value=("localhost", 1234, "test_db", "user", "anyyy"),
    )
    @patch("src.load.function.utils.connect_to_dw.Connection")
    def test_connects_to_db_success(
        self, mock_access, mock_connection, test_logger
    ):
        connect_to_db(test_logger)

        mock_access.assert_called_once_with(
            host="localhost",
            port=1234,
            database="test_db",
            user="user",
            password="anyyy",  # nosec
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
            "Connection failed: Missing one or more PostgreSQL credentials for the Warehouse.",  # noqa
        )
        assert logstream[0] == expected_log

    @patch(
        "src.load.function.utils.connect_to_dw.dw_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch(
        "src.load.function.utils.connect_to_dw.Connection",
        return_value=object(),
    )
    def test_connects_to_db_logs_info(
        self, mock_access, mock_connection, test_logger
    ):
        with LogCapture(level=logging.INFO) as logstream:
            try:
                connect_to_db(test_logger)
            except Exception:
                test_logger.error("Error in connection in test file")

        assert logstream[0] == ('test_logger',
                                'INFO',
                                'Connecting to Data Warehouse database: test_db on host: localhost') # noqa
