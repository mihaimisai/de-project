from src.load.function.utils.create_table_in_db import create_table_in_db
from unittest.mock import MagicMock
import pytest
import pandas as pd


@pytest.fixture
def dummy_dataframe():
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 22, 28, 35],
    }
    df = pd.DataFrame(data)
    return df


@pytest.fixture
def mock_conn():
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = MagicMock()
    return mock_conn


@pytest.fixture
def mock_logger():
    return MagicMock()


def test_create_table_success(mock_conn, mock_logger):
    table_name = "test_table"
    table_columns = "id INT PRIMARY KEY, name TEXT, age INT"

    create_table_in_db(mock_conn, table_name, table_columns, mock_logger)

    mock_conn.cursor.return_value.execute.assert_called_once_with(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({table_columns})"
    )

    mock_conn.commit.assert_called_once()
    mock_logger.info.assert_called_once_with(
        f"Successfully created table {table_name}"
    )
    mock_conn.cursor.close.assert_called_once()


def test_create_table_failure(mock_conn, mock_logger):
    table_name = "test_table"
    table_columns = "id INT PRIMARY KEY, name TEXT, age INT"

    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.execute.side_effect = Exception("Database error")

    with pytest.raises(Exception, match="Database error"):
        create_table_in_db(mock_conn, table_name, table_columns, mock_logger)

    mock_conn.rollback.assert_called_once()

    mock_logger.error.assert_called_once_with(
        f"General Error creating table {table_name}: Database error"
    )
    mock_cursor.close.assert_called_once()
