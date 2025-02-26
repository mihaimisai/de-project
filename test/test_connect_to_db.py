from src.utils.connect_to_db import connect_to_db, close_db
import pytest
import logging
import unittest
from unittest.mock import patch, Mock
import pg8000 
from pg8000.exceptions import InterfaceError, DatabaseError

@pytest.fixture
def test_logger():
    return logging.getLogger("test_logger")

class TestConnectToDb:
    @patch("src.utils.connect_to_db.pg_access", return_value=("localhost", 1234, "test_db", "user", "password"))
    def test_connects_to_db_fails_with_no_connection_established(self, test_logger):
        with pytest.raises(InterfaceError):
            connect_to_db(test_logger)

    @patch("src.utils.connect_to_db.pg_access", return_value=("localhost", 1234, "test_db", "user", "password"))
    @patch("src.utils.connect_to_db.Connection", return_value=object())
    def test_connects_to_db_success(self, mock_access, mock_connection, test_logger):
        result = connect_to_db(test_logger)

        mock_access.assert_called_once_with(
            host="localhost",
            port=1234,
            database="test_db",
            user="user",
            password="password"
        )
    

    # @patch("src.utils.connect_to_db.pg_access", return_value=("incorrect"))
    # def test_connects_to_db_fails_with_no_connection_established_log(self, caplog):
    #     with caplog.at_level(logging.DEBUG):
    #         try:
    #             connect_to_db(test_logger)
    #         except Exception:
    #             pass
        
    #     print(caplog)
    #     assert "Connection failed" in caplog.text
        

